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
from sqlalchemy import create_engine, text, or_, and_, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert

from models import (
    Base, Company, EnrichmentData, ProcessingLog, LinkedHelperConnection,
    PlanningData, PlanningApplication, Applicant, Officer, Appointment,
    ApplicantCompanyMatch, Contact, SharedOfficerEdge, AutomationConfig,
    AutomationRun, AutomationSchedule
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
            
            # Create enrichment spending table for budget tracking
            self.create_enrichment_spending_table()
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            raise
    
    def check_health(self) -> Dict[str, Any]:
        """Check database health and return status"""
        health_status = {
            'healthy': False,
            'database_url_configured': bool(self.database_url),
            'connection_working': False,
            'tables_accessible': False,
            'error_message': None
        }
        
        try:
            if not self.database_url:
                health_status['error_message'] = "DATABASE_URL environment variable not set"
                return health_status
            
            # Test basic connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                health_status['connection_working'] = True
                
                # Test table access
                try:
                    conn.execute(text("SELECT COUNT(*) FROM companies LIMIT 1"))
                    health_status['tables_accessible'] = True
                except Exception:
                    health_status['error_message'] = "Database tables not accessible - may need migration"
                
            health_status['healthy'] = health_status['connection_working'] and health_status['tables_accessible']
            
        except Exception as e:
            health_status['error_message'] = str(e)
            
        return health_status
    
    def get_enriched_company(self, company_number: str) -> Optional[Dict[str, Any]]:
        """Get enriched company data from cache."""
        try:
            with self.get_session() as session:
                # Query by company relationship, not direct field
                company = session.query(Company).filter(
                    Company.company_number == company_number
                ).first()
                
                if company and company.enrichment_data:
                    # Get the first enrichment record
                    enrichment = company.enrichment_data[0]
                    return enrichment.enrichment_data
                return None
        except Exception as e:
            print(f"❌ Error getting enriched company {company_number}: {e}")
            return None
    
    def store_enriched_company(self, company_number: str, enriched_data: Dict[str, Any]) -> bool:
        """Store enriched company data in cache."""
        try:
            with self.get_session() as session:
                # First get or create the company record
                company = session.query(Company).filter(
                    Company.company_number == company_number
                ).first()
                
                if not company:
                    print(f"⚠️ Company {company_number} not found in database")
                    return False
                
                # Check if enrichment already exists
                enrichment = session.query(EnrichmentData).filter(
                    EnrichmentData.company_id == company.id,
                    EnrichmentData.provider == 'companies_house'
                ).first()
                
                if enrichment:
                    enrichment.enrichment_data = enriched_data
                else:
                    enrichment = EnrichmentData(
                        company_id=company.id,
                        provider='companies_house',
                        enrichment_data=enriched_data,
                        success=True
                    )
                    session.add(enrichment)
                
                return True
        except Exception as e:
            print(f"❌ Error storing enriched company {company_number}: {e}")
            return False
    
    def create_enrichment_spending_table(self):
        """Create enrichment spending tracking table if it doesn't exist"""
        try:
            with self.get_session() as session:
                session.execute(text("""
                    CREATE TABLE IF NOT EXISTS enrichment_spending (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL,
                        amount DECIMAL(10,2) NOT NULL,
                        operation_type VARCHAR(50) NOT NULL,
                        status VARCHAR(20) DEFAULT 'confirmed',
                        confirmed_at TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_enrichment_spending_date ON enrichment_spending(date);
                """))
                session.commit()
                print("✅ Enrichment spending table created/verified")
        except Exception as e:
            print(f"⚠️ Could not create enrichment spending table: {e}")
    
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
                     search_query: Optional[str] = None) -> pd.DataFrame:
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
            
            company_dicts = [self._company_to_dict(company) for company in companies]
            return pd.DataFrame(company_dicts)
    
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
    
    def get_companies_with_enrichment(self, provider: Optional[str] = None) -> pd.DataFrame:
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
            
            return pd.DataFrame(list(companies.values()))
    
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
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """Get database statistics in the format expected by the Streamlit app"""
        with self.get_session() as session:
            # Get basic counts
            total_companies = session.query(Company).count()
            total_enrichments = session.query(EnrichmentData).count()
            
            # Count companies with successful enrichment data
            enriched_companies = session.query(Company).join(
                EnrichmentData, Company.id == EnrichmentData.company_id
            ).filter(EnrichmentData.success == True).distinct().count()
            
            # Calculate success rate
            success_rate = 0.0
            if total_companies > 0:
                success_rate = (enriched_companies / total_companies) * 100
            
            return {
                'total_companies': total_companies,
                'enriched_companies': enriched_companies,
                'success_rate': success_rate,
                'total_enrichments': total_enrichments,
                'linkedin_connections': session.query(LinkedHelperConnection).count(),
                'planning_data': session.query(PlanningData).count(),
                'planning_applications': session.query(PlanningApplication).count(),
                'processing_logs': session.query(ProcessingLog).count()
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
    
    # Missing methods needed by app.py
    
    def get_linkedhelper_stats(self) -> Dict[str, Any]:
        """Get LinkedHelper connection statistics"""
        with self.get_session() as session:
            total_connections = session.query(LinkedHelperConnection).count()
            replied_count = session.query(LinkedHelperConnection).filter(
                LinkedHelperConnection.replied.ilike('%yes%')
            ).count()
            
            # Status breakdown
            status_results = session.execute(text("""
                SELECT connection_status, COUNT(*) as count 
                FROM linkedhelper_connections 
                WHERE connection_status IS NOT NULL AND connection_status != ''
                GROUP BY connection_status
                ORDER BY count DESC
            """))
            
            status_breakdown = {row.connection_status: row.count for row in status_results}
            
            return {
                'total_connections': total_connections,
                'replied_count': replied_count,
                'status_breakdown': status_breakdown
            }
    
    def clear_linkedhelper_data(self) -> int:
        """Clear all LinkedHelper connection data"""
        with self.get_session() as session:
            deleted_count = session.query(LinkedHelperConnection).count()
            session.query(LinkedHelperConnection).delete()
            return deleted_count
    
    def check_linkedhelper_connection(self, name: str, company: str) -> bool:
        """Check if a person is already a LinkedIn connection"""
        with self.get_session() as session:
            connection = session.query(LinkedHelperConnection).filter(
                and_(
                    LinkedHelperConnection.full_name.ilike(f'%{name}%'),
                    LinkedHelperConnection.company.ilike(f'%{company}%')
                )
            ).first()
            return connection is not None
    
    def update_company(self, company_id: int, updated_data: Dict) -> bool:
        """Update existing company information"""
        with self.get_session() as session:
            company = session.query(Company).filter(Company.id == company_id).first()
            if not company:
                return False
            
            # Update fields
            for field, value in updated_data.items():
                if hasattr(company, field):
                    setattr(company, field, value)
            
            company.updated_at = datetime.now()
            return True
    
    def get_planning_data(self, company_id: int) -> List[Dict]:
        """Get planning data for a company"""
        with self.get_session() as session:
            planning_items = session.query(PlanningData).filter(
                PlanningData.company_id == company_id
            ).all()
            
            results = []
            for item in planning_items:
                results.append({
                    'id': item.id,
                    'application_type': item.application_type,
                    'decision_date': item.decision_date.isoformat() if item.decision_date else None,
                    'name': item.name,
                    'reference': item.reference,
                    'description': item.description,
                    'start_date': item.start_date.isoformat() if item.start_date else None,
                    'organisation': item.organisation,
                    'status': item.status,
                    'point': item.point,
                    'planning_url': item.planning_url,
                    'last_updated': item.last_updated.isoformat() if item.last_updated else None,
                    'created_at': item.created_at.isoformat() if item.created_at else None
                })
            
            return results
    
    def update_linkedhelper_contact(self, contact_data: Dict) -> int:
        """Update or insert LinkedHelper contact information"""
        with self.get_session() as session:
            # Try to find existing contact by LinkedIn URL or full name + company
            existing = None
            
            if contact_data.get('linkedin_url'):
                existing = session.query(LinkedHelperConnection).filter(
                    LinkedHelperConnection.linkedin_url == contact_data['linkedin_url']
                ).first()
            
            if not existing and contact_data.get('full_name') and contact_data.get('company'):
                existing = session.query(LinkedHelperConnection).filter(
                    and_(
                        LinkedHelperConnection.full_name == contact_data['full_name'],
                        LinkedHelperConnection.company == contact_data['company']
                    )
                ).first()
            
            if existing:
                # Update existing record
                for field, value in contact_data.items():
                    if hasattr(existing, field):
                        setattr(existing, field, value)
                existing.updated_at = datetime.now()
                return existing.id
            else:
                # Create new record
                return self.save_linkedin_connection(contact_data)
    
    def get_stats(self) -> Dict[str, int]:
        """Alias for get_database_stats for backward compatibility"""
        return self.get_database_stats()
    
    def save_officer(self, officer_data: Dict) -> int:
        """Save or update officer data"""
        with self.get_session() as session:
            try:
                # Try different possible keys for officer ID
                ch_officer_id = (
                    officer_data.get('officer_id') or 
                    officer_data.get('links', {}).get('officer', {}).get('appointments', '').split('/')[-1] or
                    officer_data.get('name', '').replace(' ', '_').lower() + '_' + str(officer_data.get('date_of_birth', {}).get('year', ''))
                )
                
                if not ch_officer_id:
                    raise ValueError("Officer ID is required")
                
                # Check if officer exists
                officer = session.query(Officer).filter(Officer.ch_officer_id == ch_officer_id).first()
                
                if not officer:
                    officer = Officer(ch_officer_id=ch_officer_id)
                    session.add(officer)
                
                # Update officer fields
                officer.name = officer_data.get('name', '')
                officer.nationality = officer_data.get('nationality', '')
                officer.occupation = officer_data.get('occupation', '')
                
                # Date of birth fields
                dob = officer_data.get('date_of_birth', {})
                if isinstance(dob, dict):
                    officer.date_of_birth_month = dob.get('month')
                    officer.date_of_birth_year = dob.get('year')
                
                # Address fields
                address = officer_data.get('address', {})
                if isinstance(address, dict):
                    officer.address_line_1 = address.get('address_line_1', '')
                    officer.address_line_2 = address.get('address_line_2', '')
                    officer.locality = address.get('locality', '')
                    officer.region = address.get('region', '')
                    officer.postal_code = address.get('postal_code', '')
                    officer.country = address.get('country', '')
                
                # Store raw JSON
                officer.raw_json = officer_data
                officer.updated_at = datetime.now()
                
                session.flush()
                return officer.id
                
            except Exception as e:
                session.rollback()
                raise
    
    def save_appointment(self, appointment_data: Dict) -> int:
        """Save appointment linking officer to company"""
        with self.get_session() as session:
            try:
                officer_id = appointment_data.get('officer_id')
                company_id = appointment_data.get('company_id')
                role = appointment_data.get('officer_role', '')
                
                if not officer_id or not company_id or not role:
                    raise ValueError("officer_id, company_id, and role are required")
                
                # Check for existing appointment
                existing = session.query(Appointment).filter(
                    and_(
                        Appointment.officer_id == officer_id,
                        Appointment.company_id == company_id,
                        Appointment.role == role,
                        Appointment.is_active == True
                    )
                ).first()
                
                if existing:
                    # Update existing appointment
                    existing.raw_json = appointment_data
                    existing.updated_at = datetime.now()
                    return existing.id
                
                # Create new appointment
                appointment = Appointment(
                    officer_id=officer_id,
                    company_id=company_id,
                    role=role,
                    is_active=True,
                    raw_json=appointment_data
                )
                
                # Parse dates
                if appointment_data.get('appointed_on'):
                    try:
                        appointment.appointed_date = datetime.fromisoformat(
                            appointment_data['appointed_on'].replace('Z', '+00:00')
                        ) if 'T' in appointment_data['appointed_on'] else datetime.strptime(
                            appointment_data['appointed_on'], '%Y-%m-%d'
                        )
                    except ValueError:
                        pass
                
                if appointment_data.get('resigned_on'):
                    try:
                        appointment.resigned_date = datetime.fromisoformat(
                            appointment_data['resigned_on'].replace('Z', '+00:00')
                        ) if 'T' in appointment_data['resigned_on'] else datetime.strptime(
                            appointment_data['resigned_on'], '%Y-%m-%d'
                        )
                        appointment.is_active = False
                    except ValueError:
                        pass
                
                session.add(appointment)
                session.flush()
                return appointment.id
                
            except Exception as e:
                session.rollback()
                raise
    
    def update_shared_officer_edges(self):
        """Update the shared officer edges table with current network data"""
        with self.get_session() as session:
            try:
                # Clear existing edges
                session.query(SharedOfficerEdge).delete()
                
                # Calculate shared officer counts between companies
                sql = """
                INSERT INTO shared_officer_edges (company_a_id, company_b_id, shared_officer_count, last_computed)
                SELECT 
                    a1.company_id as company_a_id,
                    a2.company_id as company_b_id,
                    COUNT(DISTINCT a1.officer_id) as shared_officer_count,
                    NOW() as last_computed
                FROM appointments a1
                JOIN appointments a2 ON a1.officer_id = a2.officer_id
                WHERE a1.company_id < a2.company_id  -- Avoid duplicates and self-loops
                    AND a1.is_active = true
                    AND a2.is_active = true
                GROUP BY a1.company_id, a2.company_id
                HAVING COUNT(DISTINCT a1.officer_id) > 0
                """
                
                session.execute(text(sql))
                session.commit()
                
                # Return count of edges created
                edge_count = session.query(SharedOfficerEdge).count()
                return edge_count
                
            except Exception as e:
                session.rollback()
                raise
    
    def get_officer_network_stats(self) -> Dict[str, int]:
        """Get statistics about the officer network"""
        with self.get_session() as session:
            return {
                'total_officers': session.query(Officer).count(),
                'total_appointments': session.query(Appointment).filter(Appointment.is_active == True).count(),
                'companies_with_officers': session.query(Appointment.company_id).filter(
                    Appointment.is_active == True
                ).distinct().count(),
                'shared_officer_edges': session.query(SharedOfficerEdge).count(),
                'max_shared_officers': session.query(func.max(SharedOfficerEdge.shared_officer_count)).scalar() or 0
            }
    
    def get_company_network(self, company_id: int, max_depth: int = 2) -> List[Dict]:
        """Get companies connected to a given company through shared officers"""
        with self.get_session() as session:
            # Find directly connected companies
            connected = session.query(SharedOfficerEdge).filter(
                or_(
                    SharedOfficerEdge.company_a_id == company_id,
                    SharedOfficerEdge.company_b_id == company_id
                )
            ).all()
            
            network = []
            for edge in connected:
                connected_company_id = edge.company_b_id if edge.company_a_id == company_id else edge.company_a_id
                
                # Get company details
                company = session.query(Company).filter(Company.id == connected_company_id).first()
                if company:
                    network.append({
                        'company_id': connected_company_id,
                        'company_number': company.company_number,
                        'company_name': company.company_name,
                        'shared_officer_count': edge.shared_officer_count,
                        'depth': 1
                    })
            
            return network
    
    # Contact Management Methods
    def upsert_contact(self, company_id: Optional[int] = None, officer_id: Optional[int] = None,
                      applicant_id: Optional[int] = None, contact_type: str = '', 
                      contact_value: str = '', source: str = '', 
                      confidence_score: Optional[float] = None,
                      verification_status: str = 'unverified') -> Dict[str, Any]:
        """Upsert (insert or update) a contact record with deduplication logic"""
        with self.get_session() as session:
            try:
                # Validate input
                if not any([company_id, officer_id, applicant_id]):
                    raise ValueError("At least one of company_id, officer_id, or applicant_id must be provided")
                
                if not contact_type or not contact_value:
                    raise ValueError("contact_type and contact_value are required")
                
                # Find existing contact
                query = session.query(Contact).filter(
                    Contact.contact_type == contact_type,
                    Contact.contact_value == contact_value
                )
                
                if company_id:
                    query = query.filter(Contact.company_id == company_id)
                if officer_id:
                    query = query.filter(Contact.officer_id == officer_id)
                if applicant_id:
                    query = query.filter(Contact.applicant_id == applicant_id)
                
                existing_contact = query.first()
                
                if existing_contact:
                    # Update existing contact
                    existing_contact.source = source
                    existing_contact.verification_status = verification_status
                    existing_contact.updated_at = datetime.now()
                    
                    if confidence_score is not None:
                        # Store confidence score in contact_value metadata if needed
                        # For now, we'll track this in logs or separate enrichment table
                        pass
                    
                    contact_id = existing_contact.id
                    created = False
                else:
                    # Create new contact
                    new_contact = Contact(
                        company_id=company_id,
                        officer_id=officer_id,
                        applicant_id=applicant_id,
                        contact_type=contact_type,
                        contact_value=contact_value,
                        source=source,
                        verification_status=verification_status
                    )
                    
                    session.add(new_contact)
                    session.flush()
                    contact_id = new_contact.id
                    created = True
                
                # Log the contact operation
                entity_id = company_id or officer_id or applicant_id
                entity_type = 'company' if company_id else ('officer' if officer_id else 'applicant')
                
                log_entry = ProcessingLog(
                    company_id=company_id if company_id else None,
                    action=f"upsert_contact_{entity_type}",
                    status="success",
                    message=f"{'Created' if created else 'Updated'} {contact_type} contact: {contact_value[:50]}..."
                )
                session.add(log_entry)
                
                return {
                    'contact_id': contact_id,
                    'created': created,
                    'updated': not created
                }
                
            except Exception as e:
                session.rollback()
                raise
    
    def get_contacts_by_entity(self, company_id: Optional[int] = None, 
                              officer_id: Optional[int] = None,
                              applicant_id: Optional[int] = None,
                              contact_type: Optional[str] = None) -> List[Dict]:
        """Get contacts for a specific entity"""
        with self.get_session() as session:
            query = session.query(Contact)
            
            if company_id:
                query = query.filter(Contact.company_id == company_id)
            if officer_id:
                query = query.filter(Contact.officer_id == officer_id)
            if applicant_id:
                query = query.filter(Contact.applicant_id == applicant_id)
            if contact_type:
                query = query.filter(Contact.contact_type == contact_type)
            
            contacts = query.order_by(Contact.updated_at.desc()).all()
            
            return [self._contact_to_dict(contact) for contact in contacts]
    
    def get_all_contacts(self, limit: Optional[int] = None, 
                        contact_type: Optional[str] = None,
                        verification_status: Optional[str] = None) -> List[Dict]:
        """Get all contacts with optional filtering"""
        with self.get_session() as session:
            query = session.query(Contact)
            
            if contact_type:
                query = query.filter(Contact.contact_type == contact_type)
            if verification_status:
                query = query.filter(Contact.verification_status == verification_status)
            
            query = query.order_by(Contact.updated_at.desc())
            
            if limit:
                query = query.limit(limit)
            
            contacts = query.all()
            return [self._contact_to_dict(contact) for contact in contacts]
    
    def delete_contact(self, contact_id: int) -> bool:
        """Delete a contact by ID"""
        with self.get_session() as session:
            contact = session.query(Contact).filter(Contact.id == contact_id).first()
            if contact:
                session.delete(contact)
                return True
            return False
    
    def get_contact_statistics(self) -> Dict[str, Any]:
        """Get contact enrichment statistics"""
        with self.get_session() as session:
            stats = {
                'total_contacts': session.query(Contact).count(),
                'contacts_by_type': {},
                'contacts_by_source': {},
                'contacts_by_verification': {},
                'companies_with_contacts': session.query(Contact.company_id).filter(
                    Contact.company_id.isnot(None)
                ).distinct().count(),
                'officers_with_contacts': session.query(Contact.officer_id).filter(
                    Contact.officer_id.isnot(None)
                ).distinct().count(),
            }
            
            # Count by contact type
            type_counts = session.query(
                Contact.contact_type, 
                func.count(Contact.id)
            ).group_by(Contact.contact_type).all()
            stats['contacts_by_type'] = {type_name: count for type_name, count in type_counts}
            
            # Count by source
            source_counts = session.query(
                Contact.source, 
                func.count(Contact.id)
            ).group_by(Contact.source).all()
            stats['contacts_by_source'] = {source: count for source, count in source_counts}
            
            # Count by verification status
            verification_counts = session.query(
                Contact.verification_status, 
                func.count(Contact.id)
            ).group_by(Contact.verification_status).all()
            stats['contacts_by_verification'] = {status: count for status, count in verification_counts}
            
            return stats
    
    def _contact_to_dict(self, contact: Contact) -> Dict:
        """Convert Contact model to dictionary"""
        return {
            'id': contact.id,
            'company_id': contact.company_id,
            'officer_id': contact.officer_id,
            'applicant_id': contact.applicant_id,
            'contact_type': contact.contact_type,
            'contact_value': contact.contact_value,
            'source': contact.source,
            'verification_status': contact.verification_status,
            'created_at': contact.created_at.isoformat() if contact.created_at else None,
            'updated_at': contact.updated_at.isoformat() if contact.updated_at else None
        }
    
    # Automation-related methods
    
    def create_automation_run(self, run_type: str, config: Optional[Dict] = None) -> int:
        """Create new automation run record"""
        with self.get_session() as session:
            run = AutomationRun(
                run_type=run_type,
                status='running',
                started_at=datetime.now()
            )
            
            if config:
                run.boroughs_processed = config.get('boroughs', [])
                run.date_range_start = config.get('date_range_start')
                run.date_range_end = config.get('date_range_end')
            
            session.add(run)
            session.flush()
            return run.id
    
    def update_automation_run(self, run_id: int, updates: Dict[str, Any]):
        """Update automation run with new statistics or status"""
        with self.get_session() as session:
            run = session.query(AutomationRun).filter(AutomationRun.id == run_id).first()
            if run:
                for key, value in updates.items():
                    if hasattr(run, key):
                        setattr(run, key, value)
                run.updated_at = datetime.now()
    
    def complete_automation_run(self, run_id: int, status: str, end_time: Optional[datetime] = None):
        """Mark automation run as completed"""
        with self.get_session() as session:
            run = session.query(AutomationRun).filter(AutomationRun.id == run_id).first()
            if run:
                run.status = status
                run.completed_at = end_time or datetime.now()
                if run.started_at:
                    run.duration_seconds = int((run.completed_at - run.started_at).total_seconds())
    
    def get_automation_runs(self, limit: int = 50, status_filter: Optional[str] = None) -> List[Dict]:
        """Get automation run history"""
        with self.get_session() as session:
            query = session.query(AutomationRun).order_by(AutomationRun.started_at.desc())
            
            if status_filter:
                query = query.filter(AutomationRun.status == status_filter)
            
            runs = query.limit(limit).all()
            return [self._automation_run_to_dict(run) for run in runs]
    
    def get_automation_statistics(self) -> Dict[str, Any]:
        """Get comprehensive automation statistics"""
        with self.get_session() as session:
            # Basic counts
            total_runs = session.query(AutomationRun).count()
            successful_runs = session.query(AutomationRun).filter(
                AutomationRun.status == 'completed'
            ).count()
            failed_runs = session.query(AutomationRun).filter(
                AutomationRun.status == 'failed'
            ).count()
            
            # Recent performance
            recent_runs = session.query(AutomationRun).filter(
                AutomationRun.started_at >= datetime.now() - timedelta(days=30)
            ).order_by(AutomationRun.started_at.desc()).limit(10).all()
            
            # Aggregate statistics
            totals = session.query(
                func.sum(AutomationRun.applications_discovered).label('total_applications'),
                func.sum(AutomationRun.applications_processed).label('total_processed'),
                func.sum(AutomationRun.companies_matched).label('total_companies'),
                func.sum(AutomationRun.contacts_enriched).label('total_contacts'),
                func.avg(AutomationRun.duration_seconds).label('avg_duration')
            ).filter(AutomationRun.status == 'completed').first()
            
            return {
                'total_runs': total_runs,
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'success_rate': (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                'recent_runs': [self._automation_run_to_dict(run) for run in recent_runs],
                'totals': {
                    'applications_discovered': totals.total_applications or 0,
                    'applications_processed': totals.total_processed or 0,
                    'companies_matched': totals.total_companies or 0,
                    'contacts_enriched': totals.total_contacts or 0,
                    'average_duration_minutes': (totals.avg_duration / 60) if totals.avg_duration else 0
                }
            }
    
    def save_automation_config(self, key: str, value: Any, description: Optional[str] = None):
        """Save automation configuration setting"""
        with self.get_session() as session:
            # Use upsert for config
            stmt = insert(AutomationConfig).values(
                config_key=key,
                config_value=json.dumps(value) if not isinstance(value, str) else value,
                description=description,
                updated_at=datetime.now()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['config_key'],
                set_=dict(
                    config_value=stmt.excluded.config_value,
                    description=stmt.excluded.description,
                    updated_at=stmt.excluded.updated_at
                )
            )
            session.execute(stmt)
    
    def get_automation_config(self, key: str, default: Any = None) -> Any:
        """Get automation configuration setting"""
        with self.get_session() as session:
            config = session.query(AutomationConfig).filter(
                AutomationConfig.config_key == key
            ).first()
            
            if config and config.config_value:
                try:
                    return json.loads(config.config_value)
                except (json.JSONDecodeError, TypeError):
                    return config.config_value
            
            return default
    
    def get_all_automation_config(self) -> Dict[str, Any]:
        """Get all automation configuration settings"""
        with self.get_session() as session:
            configs = session.query(AutomationConfig).all()
            result = {}
            
            for config in configs:
                try:
                    value = json.loads(config.config_value) if config.config_value else None
                except (json.JSONDecodeError, TypeError):
                    value = config.config_value
                
                result[config.config_key] = {
                    'value': value,
                    'description': config.description,
                    'updated_at': config.updated_at.isoformat() if config.updated_at else None
                }
            
            return result
    
    def save_automation_schedule(self, job_id: str, schedule_data: Dict[str, Any]) -> int:
        """Save automation schedule configuration"""
        with self.get_session() as session:
            # Use upsert for schedule
            stmt = insert(AutomationSchedule).values(
                job_id=job_id,
                job_name=schedule_data.get('job_name', 'Automation Job'),
                schedule_type=schedule_data.get('schedule_type', 'cron'),
                cron_expression=schedule_data.get('cron_expression'),
                interval_seconds=schedule_data.get('interval_seconds'),
                is_enabled=schedule_data.get('is_enabled', True),
                job_config=schedule_data.get('job_config', {}),
                next_execution=schedule_data.get('next_execution'),
                updated_at=datetime.now()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['job_id'],
                set_=dict(
                    job_name=stmt.excluded.job_name,
                    schedule_type=stmt.excluded.schedule_type,
                    cron_expression=stmt.excluded.cron_expression,
                    interval_seconds=stmt.excluded.interval_seconds,
                    is_enabled=stmt.excluded.is_enabled,
                    job_config=stmt.excluded.job_config,
                    next_execution=stmt.excluded.next_execution,
                    updated_at=stmt.excluded.updated_at
                )
            )
            result = session.execute(stmt)
            return result.lastrowid or 1
    
    def get_automation_schedules(self) -> List[Dict]:
        """Get all automation schedules"""
        with self.get_session() as session:
            schedules = session.query(AutomationSchedule).all()
            return [self._automation_schedule_to_dict(schedule) for schedule in schedules]
    
    def _automation_run_to_dict(self, run: AutomationRun) -> Dict:
        """Convert AutomationRun model to dictionary"""
        return {
            'id': run.id,
            'run_type': run.run_type,
            'status': run.status,
            'started_at': run.started_at.isoformat() if run.started_at else None,
            'completed_at': run.completed_at.isoformat() if run.completed_at else None,
            'duration_seconds': run.duration_seconds,
            'applications_discovered': run.applications_discovered,
            'applications_processed': run.applications_processed,
            'companies_matched': run.companies_matched,
            'companies_enriched': run.companies_enriched,
            'new_companies_created': run.new_companies_created,
            'new_officers_created': run.new_officers_created,
            'contacts_enriched': run.contacts_enriched,
            'error_count': run.error_count,
            'error_details': run.error_details,
            'boroughs_processed': run.boroughs_processed,
            'date_range_start': run.date_range_start.isoformat() if run.date_range_start else None,
            'date_range_end': run.date_range_end.isoformat() if run.date_range_end else None,
            'processing_log': run.processing_log,
            'created_at': run.created_at.isoformat() if run.created_at else None
        }
    
    def _automation_schedule_to_dict(self, schedule: AutomationSchedule) -> Dict:
        """Convert AutomationSchedule model to dictionary"""
        return {
            'id': schedule.id,
            'job_id': schedule.job_id,
            'job_name': schedule.job_name,
            'schedule_type': schedule.schedule_type,
            'cron_expression': schedule.cron_expression,
            'interval_seconds': schedule.interval_seconds,
            'is_enabled': schedule.is_enabled,
            'job_config': schedule.job_config,
            'last_run_id': schedule.last_run_id,
            'next_execution': schedule.next_execution.isoformat() if schedule.next_execution else None,
            'created_at': schedule.created_at.isoformat() if schedule.created_at else None,
            'updated_at': schedule.updated_at.isoformat() if schedule.updated_at else None
        }