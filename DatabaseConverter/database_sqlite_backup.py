import sqlite3
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import streamlit as st

class DatabaseManager:
    """Manages SQLite database operations for company data"""
    
    def __init__(self, db_path: str = "company_data.db"):
        self.db_path = db_path
    
    def init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Companies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_number TEXT UNIQUE NOT NULL,
                    company_name TEXT,
                    company_status TEXT,
                    company_type TEXT,
                    jurisdiction TEXT,
                    date_of_creation TEXT,
                    address TEXT,
                    sic_codes TEXT,
                    raw_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Enrichment data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS enrichment_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    enrichment_data TEXT,
                    success BOOLEAN DEFAULT TRUE,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES companies (id),
                    UNIQUE(company_id, provider)
                )
            """)
            
            # Processing log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES companies (id)
                )
            """)
            
            # LinkedHelper connections table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS linkedhelper_connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    full_name TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    company TEXT,
                    position TEXT,
                    linkedin_url TEXT,
                    connection_status TEXT,
                    date_connected TEXT,
                    message_sent TEXT,
                    replied TEXT,
                    tags TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Planning data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS planning_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL,
                    application_type TEXT,
                    decision_date TEXT,
                    name TEXT,
                    reference TEXT,
                    description TEXT,
                    start_date TEXT,
                    organisation TEXT,
                    status TEXT,
                    point TEXT,
                    planning_url TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES companies (id)
                )
            """)
            
            # Add planning_url column if it doesn't exist (migration)
            try:
                cursor.execute("ALTER TABLE planning_data ADD COLUMN planning_url TEXT")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            conn.commit()
    
    def save_company(self, company_data: Dict) -> int:
        """Save or update company data and return company ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Extract relevant fields
            company_number = company_data.get('company_number', '')
            company_name = company_data.get('company_name', '')
            company_status = company_data.get('company_status', '')
            company_type = company_data.get('company_type', '')
            jurisdiction = company_data.get('jurisdiction', '')
            date_of_creation = company_data.get('date_of_creation', '')
            
            # Format address
            address = ""
            if 'registered_office_address' in company_data:
                addr_data = company_data['registered_office_address']
                address_parts = []
                for key in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                    if key in addr_data and addr_data[key]:
                        address_parts.append(addr_data[key])
                address = ", ".join(address_parts)
            
            # Format SIC codes
            sic_codes = ""
            if 'sic_codes' in company_data and company_data['sic_codes']:
                sic_codes = ", ".join(company_data['sic_codes'])
            
            # Store raw data as JSON
            raw_data = json.dumps(company_data)
            
            # Insert or update company
            cursor.execute("""
                INSERT OR REPLACE INTO companies 
                (company_number, company_name, company_status, company_type, 
                 jurisdiction, date_of_creation, address, sic_codes, raw_data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_number, company_name, company_status, company_type,
                jurisdiction, date_of_creation, address, sic_codes, raw_data,
                datetime.now().isoformat()
            ))
            
            # Get company ID
            cursor.execute("SELECT id FROM companies WHERE company_number = ?", (company_number,))
            company_id = cursor.fetchone()[0]
            
            # Log the action
            cursor.execute("""
                INSERT INTO processing_log (company_id, action, status, message)
                VALUES (?, ?, ?, ?)
            """, (company_id, 'company_saved', 'success', f'Company {company_name} saved'))
            
            conn.commit()
            return company_id
    
    def save_enrichment_data(self, company_id: int, enrichment_data: Dict):
        """Save enrichment data for a company"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for provider, data in enrichment_data.items():
                if data:  # Only save non-empty data
                    cursor.execute("""
                        INSERT OR REPLACE INTO enrichment_data 
                        (company_id, provider, enrichment_data, success)
                        VALUES (?, ?, ?, ?)
                    """, (company_id, provider, json.dumps(data), True))
                else:
                    cursor.execute("""
                        INSERT OR REPLACE INTO enrichment_data 
                        (company_id, provider, enrichment_data, success, error_message)
                        VALUES (?, ?, ?, ?, ?)
                    """, (company_id, provider, None, False, f"No data returned from {provider}"))
            
            # Log the enrichment
            cursor.execute("""
                INSERT INTO processing_log (company_id, action, status, message)
                VALUES (?, ?, ?, ?)
            """, (company_id, 'data_enriched', 'success', f'Enriched with {len(enrichment_data)} providers'))
            
            conn.commit()
    
    def get_companies(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Get companies with optional filters"""
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT c.*, 
                       GROUP_CONCAT(e.provider || ': ' || e.success) as enrichment_summary,
                       GROUP_CONCAT(e.enrichment_data) as enrichment_data
                FROM companies c
                LEFT JOIN enrichment_data e ON c.id = e.company_id AND e.success = 1
            """
            
            params = []
            where_conditions = []
            
            if filters:
                if 'company_status' in filters:
                    where_conditions.append("c.company_status = ?")
                    params.append(filters['company_status'])
                
                if 'has_enrichment' in filters:
                    if filters['has_enrichment']:
                        where_conditions.append("e.id IS NOT NULL")
                    else:
                        where_conditions.append("e.id IS NULL")
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            query += " GROUP BY c.id ORDER BY c.created_at DESC"
            
            return pd.read_sql_query(query, conn, params=params)
    
    def get_company_by_number(self, company_number: str) -> Optional[Dict]:
        """Get a single company by company number"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM companies WHERE company_number = ?", (company_number,))
            result = cursor.fetchone()
            
            if result:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, result))
            
            return None
    
    def get_enrichment_data(self, company_id: int) -> Dict[str, Any]:
        """Get all enrichment data for a company"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT provider, enrichment_data, success, error_message
                FROM enrichment_data 
                WHERE company_id = ?
            """, (company_id,))
            
            enrichment_data = {}
            for row in cursor.fetchall():
                provider, data, success, error = row
                if success and data:
                    try:
                        enrichment_data[provider] = json.loads(data)
                    except json.JSONDecodeError:
                        enrichment_data[provider] = data
                else:
                    enrichment_data[provider] = {"error": error}
            
            return enrichment_data
    
    def update_company(self, company_id: int, updated_data: Dict):
        """Update company data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Update basic fields
            company_name = updated_data.get('company_name', '')
            company_status = updated_data.get('company_status', '')
            company_type = updated_data.get('company_type', '')
            
            # Format address
            address = ""
            if 'registered_office_address' in updated_data:
                addr_data = updated_data['registered_office_address']
                address_parts = []
                for key in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                    if key in addr_data and addr_data[key]:
                        address_parts.append(addr_data[key])
                address = ", ".join(address_parts)
            
            cursor.execute("""
                UPDATE companies 
                SET company_name = ?, company_status = ?, company_type = ?, 
                    address = ?, raw_data = ?, updated_at = ?
                WHERE id = ?
            """, (
                company_name, company_status, company_type, address,
                json.dumps(updated_data), datetime.now().isoformat(), company_id
            ))
            
            # Log the update
            cursor.execute("""
                INSERT INTO processing_log (company_id, action, status, message)
                VALUES (?, ?, ?, ?)
            """, (company_id, 'company_updated', 'success', 'Company data updated'))
            
            conn.commit()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Total companies
            cursor.execute("SELECT COUNT(*) FROM companies")
            total_companies = cursor.fetchone()[0]
            
            # Enriched companies
            cursor.execute("""
                SELECT COUNT(DISTINCT company_id) 
                FROM enrichment_data 
                WHERE success = 1
            """)
            enriched_companies = cursor.fetchone()[0]
            
            # Success rate
            success_rate = (enriched_companies / total_companies * 100) if total_companies > 0 else 0
            
            # Recent activity
            cursor.execute("""
                SELECT COUNT(*) FROM companies 
                WHERE created_at > datetime('now', '-7 days')
            """)
            recent_companies = cursor.fetchone()[0]
            
            return {
                'total_companies': total_companies,
                'enriched_companies': enriched_companies,
                'success_rate': success_rate,
                'recent_companies': recent_companies
            }
    
    def clear_all_data(self):
        """Clear all data from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM processing_log")
            cursor.execute("DELETE FROM enrichment_data")
            cursor.execute("DELETE FROM companies")
            
            conn.commit()
    
    def get_processing_log(self, company_id: Optional[int] = None) -> pd.DataFrame:
        """Get processing log entries"""
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT p.*, c.company_name, c.company_number
                FROM processing_log p
                JOIN companies c ON p.company_id = c.id
            """
            
            params = []
            if company_id:
                query += " WHERE p.company_id = ?"
                params.append(company_id)
            
            query += " ORDER BY p.created_at DESC LIMIT 100"
            
            return pd.read_sql_query(query, conn, params=params)
    
    def update_linkedhelper_contact(self, contact_data: Dict[str, Any]) -> bool:
        """Update or insert LinkedHelper contact from webhook data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Extract contact information
                full_name = contact_data.get('full_name', '')
                first_name = contact_data.get('first_name', '')
                last_name = contact_data.get('last_name', '')
                company = contact_data.get('company', '')
                position = contact_data.get('position', '')
                linkedin_url = contact_data.get('linkedin_url', '')
                connection_status = contact_data.get('connection_status', 'Unknown')
                date_connected = contact_data.get('date_connected', datetime.now().isoformat())
                message_sent = contact_data.get('message_sent', '')
                replied = contact_data.get('replied', 'No')
                
                # Check if contact already exists
                cursor.execute("""
                    SELECT id FROM linkedhelper_connections 
                    WHERE LOWER(full_name) = ? OR (LOWER(first_name) = ? AND LOWER(last_name) = ?)
                """, (full_name.lower(), first_name.lower(), last_name.lower()))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing contact
                    cursor.execute("""
                        UPDATE linkedhelper_connections 
                        SET connection_status = ?, date_connected = ?, message_sent = ?, 
                            replied = ?, updated_at = ?
                        WHERE id = ?
                    """, (
                        connection_status, date_connected, message_sent, 
                        replied, datetime.now().isoformat(), existing[0]
                    ))
                else:
                    # Insert new contact
                    cursor.execute("""
                        INSERT INTO linkedhelper_connections 
                        (full_name, first_name, last_name, company, position, linkedin_url, 
                         connection_status, date_connected, message_sent, replied)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        full_name, first_name, last_name, company, position, linkedin_url,
                        connection_status, date_connected, message_sent, replied
                    ))
                
                conn.commit()
                return True
                
        except Exception as e:
            return False
    
    def get_linkedhelper_stats(self) -> Dict[str, Any]:
        """Get LinkedHelper connection statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Total connections
            cursor.execute("SELECT COUNT(*) FROM linkedhelper_connections")
            total_connections = cursor.fetchone()[0]
            
            # Connected vs contacted
            cursor.execute("""
                SELECT connection_status, COUNT(*) 
                FROM linkedhelper_connections 
                WHERE connection_status != '' 
                GROUP BY connection_status
            """)
            status_breakdown = dict(cursor.fetchall())
            
            # Replied count
            cursor.execute("""
                SELECT COUNT(*) FROM linkedhelper_connections 
                WHERE replied = 'Yes' OR replied = 'True' OR replied = '1'
            """)
            replied_count = cursor.fetchone()[0]
            
            return {
                'total_connections': total_connections,
                'status_breakdown': status_breakdown,
                'replied_count': replied_count
            }
    
    def clear_linkedhelper_data(self):
        """Clear all LinkedHelper connection data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM linkedhelper_connections")
            conn.commit()
    
    def check_linkedhelper_connection(self, name: str, company: str = None) -> Dict[str, Any]:
        """Check if a person is in LinkedHelper connections"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Clean name for better matching
            clean_name = name.strip().lower()
            name_parts = clean_name.split()
            
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                
                # Search by name components
                query = """
                    SELECT full_name, company, position, connection_status, linkedin_url, replied
                    FROM linkedhelper_connections 
                    WHERE (LOWER(first_name) = ? AND LOWER(last_name) = ?)
                       OR LOWER(full_name) LIKE ?
                """
                
                params = [first_name, last_name, f"%{clean_name}%"]
                
                # Add company filter if provided
                if company:
                    query += " AND LOWER(company) LIKE ?"
                    params.append(f"%{company.lower()}%")
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                if results:
                    # Return first match with status normalization
                    result = results[0]
                    raw_status = result[3] or 'Connected'
                    
                    # Map various LinkedHelper statuses to simplified categories
                    if any(word in raw_status.lower() for word in ['connected', 'accepted', 'accept']):
                        display_status = 'Connected'
                    elif any(word in raw_status.lower() for word in ['pending', 'sent', 'request']):
                        display_status = 'Pending'
                    elif any(word in raw_status.lower() for word in ['declined', 'reject', 'ignore']):
                        display_status = 'Declined'
                    else:
                        display_status = raw_status
                    
                    return {
                        'connected': True,
                        'status': display_status,
                        'raw_status': raw_status,
                        'company': result[1],
                        'position': result[2],
                        'linkedin_url': result[4],
                        'replied': result[5],
                        'found_name': result[0]
                    }
            
            return {
                'connected': False,
                'status': 'Not Connected',
                'company': '',
                'position': '',
                'linkedin_url': '',
                'replied': '',
                'found_name': ''
            }
    
    def save_planning_data(self, company_id: int, planning_data: Dict[str, Any], resolve_urls: bool = True):
        """Save planning applications data for a company"""
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Clear existing planning data for this company
            cursor.execute("DELETE FROM planning_data WHERE company_id = ?", (company_id,))
            
            # Get planning applications
            applications = planning_data.get('applications', [])
            if not applications:
                return
            
            # Resolve URLs using the resolver service if requested
            resolved_urls = {}
            if resolve_urls and applications:
                try:
                    # Import here to avoid circular imports
                    import api_clients
                    resolver = api_clients.ResolverClient()
                    # Extract references for batch resolution
                    references = [app.get('reference', '') for app in applications if app.get('reference')]
                    if references:
                        print(f"🔍 Resolving {len(references)} planning application URLs...")
                        resolved_results = resolver.resolve_batch_items(references)
                        if resolved_results:
                            # Map references to URLs
                            for ref, url in zip(references, resolved_results):
                                if url and url != 'N/A':
                                    resolved_urls[ref] = url
                            print(f"✅ Resolved {len(resolved_urls)} planning URLs")
                        else:
                            print("❌ No URLs resolved from batch request")
                except Exception as e:
                    print(f"⚠️ URL resolution failed: {str(e)}")
                    # Don't use st.warning here as it might cause issues
                    pass
            
            # Insert each planning application with resolved URL
            for app in applications:
                reference = app.get('reference', '')
                planning_url = resolved_urls.get(reference, 'N/A') if resolve_urls else 'N/A'
                
                cursor.execute("""
                    INSERT INTO planning_data 
                    (company_id, application_type, decision_date, name, reference, description,
                     start_date, organisation, status, point, planning_url, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    company_id,
                    app.get('application_type', ''),
                    app.get('decision_date', ''),
                    app.get('name', ''),
                    reference,
                    app.get('description', ''),
                    app.get('start_date', ''),
                    app.get('organisation', ''),
                    app.get('status', ''),
                    app.get('point', ''),
                    planning_url,
                    datetime.now().isoformat()
                ))
            
            conn.commit()
            print(f"💾 Saved {len(applications)} planning applications to database")
    
    def get_planning_data(self, company_id: int) -> Dict[str, Any]:
        """Get planning applications data for a company"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT application_type, decision_date, name, reference, description,
                       start_date, organisation, status, point, planning_url, last_updated
                FROM planning_data 
                WHERE company_id = ?
                ORDER BY decision_date DESC
            """, (company_id,))
            
            results = cursor.fetchall()
            
            if results:
                applications = []
                for row in results:
                    applications.append({
                        'application_type': row[0],
                        'decision_date': row[1],
                        'name': row[2],
                        'reference': row[3],
                        'description': row[4],
                        'start_date': row[5],
                        'organisation': row[6],
                        'status': row[7],
                        'point': row[8],
                        'planning_url': row[9],
                        'last_updated': row[10]
                    })
                
                return {
                    'total_applications': len(applications),
                    'applications': applications
                }
            
            return {'total_applications': 0, 'applications': []}
