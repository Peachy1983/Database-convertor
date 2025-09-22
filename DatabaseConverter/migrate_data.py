"""
Data migration script to transfer SQLite data to PostgreSQL.
Preserves all existing data while transforming to new schema structure.
"""
import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.dialects.postgresql import insert

from database_new import DatabaseManager
from models import Company, EnrichmentData, ProcessingLog, LinkedHelperConnection, PlanningData

def migrate_sqlite_to_postgresql():
    """Migrate all data from SQLite to PostgreSQL"""
    
    # Initialize connections
    sqlite_db = "company_data.db"
    if not os.path.exists(sqlite_db):
        print("❌ SQLite database not found")
        return
    
    db_manager = DatabaseManager()
    
    print("🔄 Starting data migration from SQLite to PostgreSQL...")
    
    try:
        # Test PostgreSQL connection
        db_manager.init_database()
        
        # Connect to SQLite
        sqlite_conn = sqlite3.connect(sqlite_db)
        sqlite_conn.row_factory = sqlite3.Row  # For dict-like access
        
        # Migrate data in order (respecting foreign key constraints)
        migrate_companies(sqlite_conn, db_manager)
        migrate_enrichment_data(sqlite_conn, db_manager)
        migrate_processing_logs(sqlite_conn, db_manager)
        migrate_linkedin_connections(sqlite_conn, db_manager)
        migrate_planning_data(sqlite_conn, db_manager)
        
        sqlite_conn.close()
        
        # Verify migration
        stats = db_manager.get_database_stats()
        print(f"\n✅ Migration completed successfully!")
        print("\n📊 PostgreSQL Database Statistics:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise

def migrate_companies(sqlite_conn, db_manager: DatabaseManager):
    """Migrate companies table"""
    print("\n🏢 Migrating companies...")
    
    cursor = sqlite_conn.execute("SELECT * FROM companies ORDER BY id")
    companies = cursor.fetchall()
    
    migrated = 0
    failed = 0
    
    with db_manager.get_session() as session:
        for company_row in companies:
            try:
                # Parse raw data if it exists
                raw_data = {}
                if company_row['raw_data']:
                    try:
                        raw_data = json.loads(company_row['raw_data'])
                    except json.JSONDecodeError:
                        raw_data = {'original_raw_data': company_row['raw_data']}
                
                # Create company using upsert
                stmt = insert(Company).values(
                    company_number=company_row['company_number'] or '',
                    company_name=company_row['company_name'] or '',
                    company_status=company_row['company_status'] or '',
                    company_type=company_row['company_type'] or '',
                    jurisdiction=company_row['jurisdiction'] or '',
                    raw_json=raw_data,
                    created_at=parse_date(company_row['created_at']),
                    updated_at=parse_date(company_row['updated_at'])
                )
                
                # Parse date of creation
                if company_row['date_of_creation']:
                    stmt = stmt.values(date_of_creation=parse_date(company_row['date_of_creation']))
                
                # Parse address from raw data or existing field
                if raw_data.get('registered_office_address'):
                    addr = raw_data['registered_office_address']
                    stmt = stmt.values(
                        address_line_1=addr.get('address_line_1', ''),
                        address_line_2=addr.get('address_line_2', ''),
                        locality=addr.get('locality', ''),
                        region=addr.get('region', ''),
                        postal_code=addr.get('postal_code', ''),
                        country=addr.get('country', '')
                    )
                elif company_row['address']:
                    # Use existing address as address_line_1
                    stmt = stmt.values(address_line_1=company_row['address'])
                
                # Parse SIC codes
                if company_row['sic_codes']:
                    if company_row['sic_codes'].startswith('['):
                        # Already JSON array
                        try:
                            sic_list = json.loads(company_row['sic_codes'])
                            stmt = stmt.values(sic_codes=sic_list)
                        except json.JSONDecodeError:
                            # Comma separated
                            sic_list = [code.strip() for code in company_row['sic_codes'].split(',')]
                            stmt = stmt.values(sic_codes=sic_list)
                    else:
                        # Comma separated
                        sic_list = [code.strip() for code in company_row['sic_codes'].split(',')]
                        stmt = stmt.values(sic_codes=sic_list)
                elif raw_data.get('sic_codes'):
                    stmt = stmt.values(sic_codes=raw_data['sic_codes'])
                
                # Handle conflicts
                stmt = stmt.on_conflict_do_update(
                    index_elements=['company_number'],
                    set_=dict(
                        company_name=stmt.excluded.company_name,
                        company_status=stmt.excluded.company_status,
                        company_type=stmt.excluded.company_type,
                        jurisdiction=stmt.excluded.jurisdiction,
                        date_of_creation=stmt.excluded.date_of_creation,
                        address_line_1=stmt.excluded.address_line_1,
                        address_line_2=stmt.excluded.address_line_2,
                        locality=stmt.excluded.locality,
                        region=stmt.excluded.region,
                        postal_code=stmt.excluded.postal_code,
                        country=stmt.excluded.country,
                        sic_codes=stmt.excluded.sic_codes,
                        raw_json=stmt.excluded.raw_json,
                        updated_at=stmt.excluded.updated_at
                    )
                )
                
                session.execute(stmt)
                migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate company {company_row['company_number']}: {e}")
                failed += 1
    
    print(f"  ✅ Companies migrated: {migrated}, failed: {failed}")

def migrate_enrichment_data(sqlite_conn, db_manager: DatabaseManager):
    """Migrate enrichment_data table"""
    print("\n🔍 Migrating enrichment data...")
    
    cursor = sqlite_conn.execute("""
        SELECT e.*, c.company_number 
        FROM enrichment_data e 
        JOIN companies c ON e.company_id = c.id
        ORDER BY e.id
    """)
    enrichments = cursor.fetchall()
    
    migrated = 0
    failed = 0
    
    with db_manager.get_session() as session:
        for enrichment_row in enrichments:
            try:
                # Find the new company ID
                company = session.query(Company).filter(
                    Company.company_number == enrichment_row['company_number']
                ).first()
                
                if not company:
                    print(f"  ⚠️ Company {enrichment_row['company_number']} not found, skipping enrichment")
                    failed += 1
                    continue
                
                # Parse enrichment data
                enrichment_data = {}
                if enrichment_row['enrichment_data']:
                    try:
                        enrichment_data = json.loads(enrichment_row['enrichment_data'])
                    except json.JSONDecodeError:
                        enrichment_data = {'original_data': enrichment_row['enrichment_data']}
                
                # Create enrichment using upsert
                stmt = insert(EnrichmentData).values(
                    company_id=company.id,
                    provider=enrichment_row['provider'] or 'unknown',
                    enrichment_data=enrichment_data,
                    success=bool(enrichment_row['success']),
                    error_message=enrichment_row['error_message'],
                    created_at=parse_date(enrichment_row['created_at'])
                )
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['company_id', 'provider'],
                    set_=dict(
                        enrichment_data=stmt.excluded.enrichment_data,
                        success=stmt.excluded.success,
                        error_message=stmt.excluded.error_message,
                        created_at=stmt.excluded.created_at
                    )
                )
                
                session.execute(stmt)
                migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate enrichment {enrichment_row['id']}: {e}")
                failed += 1
    
    print(f"  ✅ Enrichment data migrated: {migrated}, failed: {failed}")

def migrate_processing_logs(sqlite_conn, db_manager: DatabaseManager):
    """Migrate processing_log table"""
    print("\n📋 Migrating processing logs...")
    
    cursor = sqlite_conn.execute("""
        SELECT p.*, c.company_number 
        FROM processing_log p 
        JOIN companies c ON p.company_id = c.id
        ORDER BY p.id
    """)
    logs = cursor.fetchall()
    
    migrated = 0
    failed = 0
    
    with db_manager.get_session() as session:
        for log_row in logs:
            try:
                # Find the new company ID
                company = session.query(Company).filter(
                    Company.company_number == log_row['company_number']
                ).first()
                
                if not company:
                    print(f"  ⚠️ Company {log_row['company_number']} not found, skipping log")
                    failed += 1
                    continue
                
                # Create processing log
                log_entry = ProcessingLog(
                    company_id=company.id,
                    action=log_row['action'] or 'unknown',
                    status=log_row['status'] or 'unknown',
                    message=log_row['message'],
                    created_at=parse_date(log_row['created_at'])
                )
                
                session.add(log_entry)
                migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate log {log_row['id']}: {e}")
                failed += 1
    
    print(f"  ✅ Processing logs migrated: {migrated}, failed: {failed}")

def migrate_linkedin_connections(sqlite_conn, db_manager: DatabaseManager):
    """Migrate linkedhelper_connections table"""
    print("\n🔗 Migrating LinkedIn connections...")
    
    cursor = sqlite_conn.execute("SELECT * FROM linkedhelper_connections ORDER BY id")
    connections = cursor.fetchall()
    
    migrated = 0
    failed = 0
    
    with db_manager.get_session() as session:
        for conn_row in connections:
            try:
                connection = LinkedHelperConnection(
                    full_name=conn_row['full_name'] or '',
                    first_name=conn_row['first_name'] or '',
                    last_name=conn_row['last_name'] or '',
                    company=conn_row['company'] or '',
                    position=conn_row['position'] or '',
                    linkedin_url=conn_row['linkedin_url'] or '',
                    connection_status=conn_row['connection_status'] or '',
                    date_connected=parse_date(conn_row['date_connected']),
                    message_sent=conn_row['message_sent'] or '',
                    replied=conn_row['replied'] or '',
                    tags=conn_row['tags'] or '',
                    notes=conn_row['notes'] or '',
                    created_at=parse_date(conn_row['created_at']),
                    updated_at=parse_date(conn_row['updated_at'])
                )
                
                session.add(connection)
                migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate LinkedIn connection {conn_row['id']}: {e}")
                failed += 1
    
    print(f"  ✅ LinkedIn connections migrated: {migrated}, failed: {failed}")

def migrate_planning_data(sqlite_conn, db_manager: DatabaseManager):
    """Migrate planning_data table"""
    print("\n🏗️ Migrating planning data...")
    
    cursor = sqlite_conn.execute("""
        SELECT p.*, c.company_number 
        FROM planning_data p 
        JOIN companies c ON p.company_id = c.id
        ORDER BY p.id
    """)
    planning_items = cursor.fetchall()
    
    migrated = 0
    failed = 0
    
    with db_manager.get_session() as session:
        for planning_row in planning_items:
            try:
                # Find the new company ID
                company = session.query(Company).filter(
                    Company.company_number == planning_row['company_number']
                ).first()
                
                if not company:
                    print(f"  ⚠️ Company {planning_row['company_number']} not found, skipping planning data")
                    failed += 1
                    continue
                
                # Create planning data
                planning = PlanningData(
                    company_id=company.id,
                    application_type=planning_row['application_type'] or '',
                    decision_date=parse_date(planning_row['decision_date']),
                    name=planning_row['name'] or '',
                    reference=planning_row['reference'] or '',
                    description=planning_row['description'] or '',
                    start_date=parse_date(planning_row['start_date']),
                    organisation=planning_row['organisation'] or '',
                    status=planning_row['status'] or '',
                    point=planning_row['point'] or '',
                    planning_url=planning_row['planning_url'] or '',
                    last_updated=parse_date(planning_row['last_updated']),
                    created_at=parse_date(planning_row['created_at'])
                )
                
                session.add(planning)
                migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate planning data {planning_row['id']}: {e}")
                failed += 1
    
    print(f"  ✅ Planning data migrated: {migrated}, failed: {failed}")

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string to datetime object"""
    if not date_str:
        return None
    
    # Try different date formats
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # If all fail, try parsing as ISO format
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        print(f"  ⚠️ Could not parse date: {date_str}")
        return None

if __name__ == "__main__":
    migrate_sqlite_to_postgresql()