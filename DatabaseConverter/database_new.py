"""
PostgreSQL database manager using SQLAlchemy.
Comprehensive database operations for the developer-lender intelligence system.
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import streamlit as st
from contextlib import contextmanager
from sqlalchemy import create_engine, text, or_, and_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert

from models import (
    Base, Company, EnrichmentData, ProcessingLog, LinkedHelperConnection,
    PlanningData, PlanningApplication, Applicant, Officer, Appointment,
    ApplicantCompanyMatch, Contact, SharedOfficerEdge
)

class DatabaseManager:
    """Manages PostgreSQL database operations for company data using SQLAlchemy"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        # Create engine with connection pooling
        self.engine = create_engine(
            self.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=300
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    def init_database(self):
        """Initialize database - schema already created via Alembic"""
        try:
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL database connection successful")
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            raise
    
    def save_company(self, company_data: Dict) -> int:
        """Save or update company data and return company ID"""
        with self.get_session() as session:
            try:
                # Extract and prepare company data
                company_number = company_data.get('company_number', '')
                if not company_number:
                    raise ValueError("Company number is required")
                
                # Check if company exists
                company = session.query(Company).filter(Company.company_number == company_number).first()
                
                if not company:
                    company = Company(company_number=company_number)
                    session.add(company)
                
                # Update company fields
                company.company_name = company_data.get('company_name', '')
                company.company_status = company_data.get('company_status', '')
                company.company_type = company_data.get('company_type', '')
                company.jurisdiction = company_data.get('jurisdiction', '')
                
                # Parse date fields
                if company_data.get('date_of_creation'):
                    try:
                        company.date_of_creation = datetime.fromisoformat(
                            company_data['date_of_creation'].replace('Z', '+00:00')
                        ) if 'T' in company_data['date_of_creation'] else datetime.strptime(
                            company_data['date_of_creation'], '%Y-%m-%d'
                        )
                    except ValueError:
                        pass
                
                # Handle address
                if 'registered_office_address' in company_data:
                    addr = company_data['registered_office_address']
                    company.address_line_1 = addr.get('address_line_1', '')
                    company.address_line_2 = addr.get('address_line_2', '')
                    company.locality = addr.get('locality', '')
                    company.region = addr.get('region', '')
                    company.postal_code = addr.get('postal_code', '')
                    company.country = addr.get('country', '')
                
                # Handle SIC codes
                if 'sic_codes' in company_data and company_data['sic_codes']:
                    company.sic_codes = company_data['sic_codes']
                
                # Store raw JSON
                company.raw_json = company_data
                company.updated_at = datetime.now()
                
                session.flush()  # Get the ID
                
                # Log the action
                log_entry = ProcessingLog(
                    company_id=company.id,
                    action="save_company",
                    status="success",
                    message=f"Company {company_number} saved successfully"
                )
                session.add(log_entry)
                
                return company.id
                
            except Exception as e:
                # Log the error
                if 'company' in locals() and company.id:
                    log_entry = ProcessingLog(
                        company_id=company.id,
                        action="save_company",
                        status="error",
                        message=str(e)
                    )
                    session.add(log_entry)
                raise
    
    def save_enrichment_data(self, company_id: int, provider: str, 
                           enrichment_data: Dict, success: bool = True, 
                           error_message: Optional[str] = None) -> int:
        """Save enrichment data"""
        with self.get_session() as session:
            # Use upsert to handle duplicates
            stmt = insert(EnrichmentData).values(
                company_id=company_id,
                provider=provider,
                enrichment_data=enrichment_data,
                success=success,
                error_message=error_message,
                created_at=datetime.now()
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
            result = session.execute(stmt)
            session.commit()
            
            # Log the action
            log_entry = ProcessingLog(
                company_id=company_id,
                action="save_enrichment",
                status="success" if success else "error",
                message=f"Enrichment data from {provider} {'saved' if success else 'failed'}"
            )
            session.add(log_entry)
            
            return result.lastrowid or company_id
    
    def get_companies(self, limit: Optional[int] = None, 
                     search_query: Optional[str] = None) -> List[Dict]:
        """Retrieve companies with optional search and limit"""
        with self.get_session() as session:
            query = session.query(Company)
            
            if search_query:
                search = f"%{search_query}%"
                query = query.filter(
                    or_(
                        Company.company_name.ilike(search),
                        Company.company_number.ilike(search),
                        Company.address_line_1.ilike(search)
                    )
                )
            
            query = query.order_by(Company.updated_at.desc())
            
            if limit:
                query = query.limit(limit)
            
            companies = query.all()
            
            return [self._company_to_dict(company) for company in companies]
    
    def get_company_by_number(self, company_number: str) -> Optional[Dict]:
        """Get company by company number"""
        with self.get_session() as session:
            company = session.query(Company).filter(
                Company.company_number == company_number
            ).first()
            
            if company:
                return self._company_to_dict(company)
            return None
    
    def get_enrichment_data(self, company_id: int, 
                          provider: Optional[str] = None) -> List[Dict]:
        """Get enrichment data for a company"""
        with self.get_session() as session:
            query = session.query(EnrichmentData).filter(
                EnrichmentData.company_id == company_id
            )
            
            if provider:
                query = query.filter(EnrichmentData.provider == provider)
            
            enrichments = query.all()
            
            return [self._enrichment_to_dict(enrichment) for enrichment in enrichments]
    
    def get_companies_with_enrichment(self, provider: Optional[str] = None) -> List[Dict]:
        """Get companies along with their enrichment data"""
        with self.get_session() as session:
            query = session.query(Company, EnrichmentData).outerjoin(
                EnrichmentData, Company.id == EnrichmentData.company_id
            )
            
            if provider:
                query = query.filter(
                    or_(
                        EnrichmentData.provider == provider,
                        EnrichmentData.provider.is_(None)
                    )
                )
            
            results = query.all()
            
            companies = {}
            for company, enrichment in results:
                company_dict = self._company_to_dict(company)
                company_id = company.id
                
                if company_id not in companies:
                    companies[company_id] = company_dict
                    companies[company_id]['enrichment_data'] = {}
                
                if enrichment:
                    companies[company_id]['enrichment_data'][enrichment.provider] = {
                        'data': enrichment.enrichment_data,
                        'success': enrichment.success,
                        'error_message': enrichment.error_message,
                        'created_at': enrichment.created_at.isoformat() if enrichment.created_at else None
                    }
            
            return list(companies.values())
    
    def save_linkedin_connection(self, connection_data: Dict) -> int:
        """Save LinkedIn connection data"""
        with self.get_session() as session:
            # Create or update connection
            connection = LinkedHelperConnection(
                full_name=connection_data.get('full_name', ''),
                first_name=connection_data.get('first_name', ''),
                last_name=connection_data.get('last_name', ''),
                company=connection_data.get('company', ''),
                position=connection_data.get('position', ''),
                linkedin_url=connection_data.get('linkedin_url', ''),
                connection_status=connection_data.get('connection_status', ''),
                message_sent=connection_data.get('message_sent', ''),
                replied=connection_data.get('replied', ''),
                tags=connection_data.get('tags', ''),
                notes=connection_data.get('notes', ''),
                updated_at=datetime.now()
            )
            
            # Parse date connected
            if connection_data.get('date_connected'):
                try:
                    connection.date_connected = datetime.fromisoformat(
                        connection_data['date_connected']
                    )
                except ValueError:
                    pass
            
            session.add(connection)
            session.flush()
            
            return connection.id
    
    def get_linkedin_connections(self, limit: Optional[int] = None) -> List[Dict]:
        """Get LinkedIn connections"""
        with self.get_session() as session:
            query = session.query(LinkedHelperConnection).order_by(
                LinkedHelperConnection.updated_at.desc()
            )
            
            if limit:
                query = query.limit(limit)
            
            connections = query.all()
            
            return [self._linkedin_connection_to_dict(conn) for conn in connections]
    
    def save_planning_data(self, company_id: int, planning_data: Dict) -> int:
        """Save planning data for a company"""
        with self.get_session() as session:
            planning = PlanningData(
                company_id=company_id,
                application_type=planning_data.get('application_type', ''),
                name=planning_data.get('name', ''),
                reference=planning_data.get('reference', ''),
                description=planning_data.get('description', ''),
                organisation=planning_data.get('organisation', ''),
                status=planning_data.get('status', ''),
                point=planning_data.get('point', ''),
                planning_url=planning_data.get('planning_url', ''),
                last_updated=datetime.now()
            )
            
            # Parse dates
            for date_field in ['decision_date', 'start_date']:
                if planning_data.get(date_field):
                    try:
                        setattr(planning, date_field, datetime.fromisoformat(
                            planning_data[date_field]
                        ))
                    except ValueError:
                        pass
            
            session.add(planning)
            session.flush()
            
            return planning.id
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        with self.get_session() as session:
            return {
                'companies': session.query(Company).count(),
                'enrichment_data': session.query(EnrichmentData).count(),
                'processing_logs': session.query(ProcessingLog).count(),
                'linkedin_connections': session.query(LinkedHelperConnection).count(),
                'planning_data': session.query(PlanningData).count(),
                'planning_applications': session.query(PlanningApplication).count(),
                'applicants': session.query(Applicant).count(),
                'officers': session.query(Officer).count(),
                'appointments': session.query(Appointment).count()
            }
    
    def export_to_dataframe(self, table_name: str) -> pd.DataFrame:
        """Export table data to pandas DataFrame"""
        with self.engine.connect() as conn:
            return pd.read_sql_table(table_name, conn)
    
    def execute_raw_sql(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute raw SQL query"""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result]
    
    # Helper methods for data conversion
    def _company_to_dict(self, company: Company) -> Dict:
        """Convert Company object to dictionary"""
        return {
            'id': company.id,
            'company_number': company.company_number,
            'company_name': company.company_name,
            'company_status': company.company_status,
            'company_type': company.company_type,
            'jurisdiction': company.jurisdiction,
            'date_of_creation': company.date_of_creation.isoformat() if company.date_of_creation else None,
            'address': self._format_address(company),
            'sic_codes': company.sic_codes or [],
            'raw_data': json.dumps(company.raw_json or {}),
            'created_at': company.created_at.isoformat() if company.created_at else None,
            'updated_at': company.updated_at.isoformat() if company.updated_at else None
        }
    
    def _format_address(self, company: Company) -> str:
        """Format address from company fields"""
        address_parts = []
        for field in [company.address_line_1, company.address_line_2, 
                     company.locality, company.region, company.postal_code, company.country]:
            if field:
                address_parts.append(field)
        return ", ".join(address_parts)
    
    def _enrichment_to_dict(self, enrichment: EnrichmentData) -> Dict:
        """Convert EnrichmentData object to dictionary"""
        return {
            'id': enrichment.id,
            'company_id': enrichment.company_id,
            'provider': enrichment.provider,
            'enrichment_data': enrichment.enrichment_data,
            'success': enrichment.success,
            'error_message': enrichment.error_message,
            'created_at': enrichment.created_at.isoformat() if enrichment.created_at else None
        }
    
    def _linkedin_connection_to_dict(self, connection: LinkedHelperConnection) -> Dict:
        """Convert LinkedHelperConnection object to dictionary"""
        return {
            'id': connection.id,
            'full_name': connection.full_name,
            'first_name': connection.first_name,
            'last_name': connection.last_name,
            'company': connection.company,
            'position': connection.position,
            'linkedin_url': connection.linkedin_url,
            'connection_status': connection.connection_status,
            'date_connected': connection.date_connected.isoformat() if connection.date_connected else None,
            'message_sent': connection.message_sent,
            'replied': connection.replied,
            'tags': connection.tags,
            'notes': connection.notes,
            'created_at': connection.created_at.isoformat() if connection.created_at else None,
            'updated_at': connection.updated_at.isoformat() if connection.updated_at else None
        }
    
    # New methods for planning applications and officer networks
    
    def save_planning_application(self, application_data: Dict) -> int:
        """Save a planning application"""
        with self.get_session() as session:
            # Use upsert to handle duplicates
            stmt = insert(PlanningApplication).values(
                borough=application_data.get('borough', ''),
                reference=application_data.get('reference', ''),
                application_type=application_data.get('application_type', ''),
                status=application_data.get('status', ''),
                description=application_data.get('description', ''),
                is_outline=application_data.get('is_outline', False),
                latitude=application_data.get('latitude'),
                longitude=application_data.get('longitude'),
                planning_url=application_data.get('planning_url', ''),
                organisation=application_data.get('organisation', ''),
                raw_data=application_data,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Add date parsing
            for date_field in ['decision_date', 'received_date', 'start_date']:
                if application_data.get(date_field):
                    try:
                        stmt = stmt.values(**{date_field: datetime.fromisoformat(
                            application_data[date_field]
                        )})
                    except ValueError:
                        pass
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['borough', 'reference'],
                set_=dict(
                    application_type=stmt.excluded.application_type,
                    status=stmt.excluded.status,
                    description=stmt.excluded.description,
                    updated_at=stmt.excluded.updated_at,
                    raw_data=stmt.excluded.raw_data
                )
            )
            
            result = session.execute(stmt)
            return result.lastrowid or 1
    
    def save_officer(self, officer_data: Dict) -> int:
        """Save company officer data"""
        with self.get_session() as session:
            # Use upsert for officers
            stmt = insert(Officer).values(
                ch_officer_id=officer_data.get('links', {}).get('officer', {}).get('appointments', '').split('/')[-2] if officer_data.get('links', {}).get('officer', {}).get('appointments') else None,
                name=officer_data.get('name', ''),
                nationality=officer_data.get('nationality', ''),
                occupation=officer_data.get('occupation', ''),
                raw_json=officer_data,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Add date of birth if available
            if officer_data.get('date_of_birth'):
                dob = officer_data['date_of_birth']
                stmt = stmt.values(
                    date_of_birth_month=dob.get('month'),
                    date_of_birth_year=dob.get('year')
                )
            
            # Add address if available
            if officer_data.get('address'):
                addr = officer_data['address']
                stmt = stmt.values(
                    address_line_1=addr.get('address_line_1', ''),
                    address_line_2=addr.get('address_line_2', ''),
                    locality=addr.get('locality', ''),
                    region=addr.get('region', ''),
                    postal_code=addr.get('postal_code', ''),
                    country=addr.get('country', '')
                )
            
            if officer_data.get('links', {}).get('officer', {}).get('appointments'):
                stmt = stmt.on_conflict_do_update(
                    index_elements=['ch_officer_id'],
                    set_=dict(
                        name=stmt.excluded.name,
                        nationality=stmt.excluded.nationality,
                        occupation=stmt.excluded.occupation,
                        updated_at=stmt.excluded.updated_at,
                        raw_json=stmt.excluded.raw_json
                    )
                )
            
            result = session.execute(stmt)
            return result.lastrowid or 1
    
    def get_shared_officer_networks(self, company_id: int, 
                                   min_shared_officers: int = 1) -> List[Dict]:
        """Get companies connected by shared officers"""
        with self.get_session() as session:
            # This would typically use the precomputed shared_officer_edges table
            # For now, compute on-the-fly
            query = """
            SELECT 
                c2.id as connected_company_id,
                c2.company_name,
                c2.company_number,
                COUNT(DISTINCT a1.officer_id) as shared_officers
            FROM appointments a1
            JOIN appointments a2 ON a1.officer_id = a2.officer_id 
                AND a1.company_id != a2.company_id
            JOIN companies c2 ON a2.company_id = c2.id
            WHERE a1.company_id = :company_id 
                AND a1.is_active = true 
                AND a2.is_active = true
            GROUP BY c2.id, c2.company_name, c2.company_number
            HAVING COUNT(DISTINCT a1.officer_id) >= :min_shared
            ORDER BY shared_officers DESC
            """
            
            result = session.execute(
                text(query), 
                {'company_id': company_id, 'min_shared': min_shared_officers}
            )
            
            return [dict(row._mapping) for row in result]