"""
Complete pipeline integration for processing planning applicants.
Handles: Raw applicants → Company matching → Companies House lookup → Officer extraction → Database storage
"""
import os
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import DatabaseManager
from applicant_processor import ApplicantProcessor, CompanyMatch
from api_clients import CompaniesHouseClient
from contact_enrichment import ContactEnrichmentPipeline
from models import Applicant, Company, Officer, Appointment, ApplicantCompanyMatch

logger = logging.getLogger(__name__)

class ApplicantPipeline:
    """Complete pipeline for processing planning applicants and building officer networks"""
    
    def __init__(self, db_manager: DatabaseManager, companies_house_key: str, 
                 brightdata_key: str = None, hunter_key: str = None, 
                 enable_contact_enrichment: bool = True):
        self.db_manager = db_manager
        self.applicant_processor = ApplicantProcessor()
        self.companies_house = CompaniesHouseClient(companies_house_key)
        
        # Initialize contact enrichment pipeline
        self.enable_contact_enrichment = enable_contact_enrichment
        if self.enable_contact_enrichment:
            self.contact_enrichment = ContactEnrichmentPipeline(
                db_manager, brightdata_key, hunter_key
            )
        else:
            self.contact_enrichment = None
        
        # Pipeline configuration
        self.batch_size = 10  # Process companies in batches to manage API rate limits
        self.min_confidence_score = 0.7  # Minimum confidence for auto-processing matches
        self.max_matches_per_applicant = 3  # Limit matches to avoid excessive API calls
    
    def process_applicant_batch(self, applicants: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a complete batch of applicants through the full pipeline"""
        pipeline_stats = {
            'total_applicants': len(applicants),
            'processed_applicants': 0,
            'matched_companies': 0,
            'new_companies_fetched': 0,
            'new_officers_fetched': 0,
            'new_appointments_created': 0,
            'network_edges_updated': 0,
            'contact_enrichment_enabled': self.enable_contact_enrichment,
            'companies_enriched': 0,
            'linkedin_profiles_found': 0,
            'emails_discovered': 0,
            'contacts_created': 0,
            'errors': []
        }
        
        try:
            logger.info(f"Starting pipeline processing for {len(applicants)} applicants")
            
            # Step 1: Validate and normalize applicant data
            validated_applicants = []
            for applicant_data in applicants:
                is_valid, validation_msg = self.applicant_processor.validate_applicant_data(applicant_data)
                if is_valid:
                    normalized = self.applicant_processor.normalize_applicant_data(applicant_data)
                    validated_applicants.append(normalized)
                else:
                    pipeline_stats['errors'].append(f"Validation failed: {validation_msg}")
            
            logger.info(f"Validated {len(validated_applicants)} applicants")
            
            # Step 2: Deduplicate applicants
            deduplicated_applicants = self.applicant_processor.deduplicate_applicants(validated_applicants)
            logger.info(f"After deduplication: {len(deduplicated_applicants)} unique applicants")
            
            # Step 3: Process each applicant through the pipeline
            for applicant_data in deduplicated_applicants:
                try:
                    result = self._process_single_applicant(applicant_data)
                    
                    pipeline_stats['processed_applicants'] += 1
                    pipeline_stats['matched_companies'] += result.get('matched_companies', 0)
                    pipeline_stats['new_companies_fetched'] += result.get('new_companies_fetched', 0)
                    pipeline_stats['new_officers_fetched'] += result.get('new_officers_fetched', 0)
                    pipeline_stats['new_appointments_created'] += result.get('new_appointments_created', 0)
                    
                except Exception as e:
                    error_msg = f"Error processing applicant {applicant_data.get('raw_name', 'Unknown')}: {str(e)}"
                    pipeline_stats['errors'].append(error_msg)
                    logger.error(error_msg)
            
            # Step 4: Update officer network edges
            try:
                edge_count = self.db_manager.update_shared_officer_edges()
                pipeline_stats['network_edges_updated'] = edge_count
                logger.info(f"Updated {edge_count} officer network edges")
            except Exception as e:
                error_msg = f"Failed to update officer network: {str(e)}"
                pipeline_stats['errors'].append(error_msg)
                logger.error(error_msg)
            
            # Step 5: Contact enrichment for processed companies
            if self.enable_contact_enrichment and self.contact_enrichment:
                try:
                    enriched_companies = self._run_contact_enrichment_batch(pipeline_stats)
                    pipeline_stats['companies_enriched'] = enriched_companies['companies_enriched']
                    pipeline_stats['linkedin_profiles_found'] = enriched_companies['linkedin_profiles_found']
                    pipeline_stats['emails_discovered'] = enriched_companies['emails_discovered']
                    pipeline_stats['contacts_created'] = enriched_companies['contacts_created']
                    
                    logger.info(f"Contact enrichment completed. Enriched {enriched_companies['companies_enriched']} companies, "
                              f"found {enriched_companies['linkedin_profiles_found']} LinkedIn profiles, "
                              f"{enriched_companies['emails_discovered']} email addresses")
                    
                except Exception as e:
                    error_msg = f"Contact enrichment failed: {str(e)}"
                    pipeline_stats['errors'].append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Pipeline completed. Processed {pipeline_stats['processed_applicants']} applicants with {len(pipeline_stats['errors'])} errors")
            
        except Exception as e:
            error_msg = f"Pipeline fatal error: {str(e)}"
            pipeline_stats['errors'].append(error_msg)
            logger.error(error_msg)
        
        return pipeline_stats
    
    def _process_single_applicant(self, applicant_data: Dict[str, Any]) -> Dict[str, int]:
        """Process a single applicant through the complete pipeline"""
        result = {
            'matched_companies': 0,
            'new_companies_fetched': 0,
            'new_officers_fetched': 0,
            'new_appointments_created': 0
        }
        
        try:
            with self.db_manager.get_session() as session:
                # Create or get applicant record
                applicant = self._create_applicant_record(session, applicant_data)
                
                # Skip if this is an individual
                if applicant_data.get('applicant_type') == 'individual':
                    logger.debug(f"Skipping individual applicant: {applicant_data['raw_name']}")
                    return result
                
                # Step 1: Search for potential company matches
                potential_companies = self._search_potential_companies(applicant_data['raw_name'])
                
                if not potential_companies:
                    logger.debug(f"No company matches found for: {applicant_data['raw_name']}")
                    return result
                
                # Step 2: Find fuzzy matches
                matches = self.applicant_processor.find_potential_matches(
                    applicant_data['raw_name'], 
                    potential_companies
                )
                
                # Filter by confidence and limit results
                high_confidence_matches = [
                    m for m in matches 
                    if m.confidence_score >= self.min_confidence_score
                ][:self.max_matches_per_applicant]
                
                if not high_confidence_matches:
                    logger.debug(f"No high-confidence matches for: {applicant_data['raw_name']}")
                    return result
                
                # Step 3: Process each match
                for match in high_confidence_matches:
                    try:
                        match_result = self._process_company_match(session, applicant, match)
                        
                        result['matched_companies'] += 1
                        result['new_companies_fetched'] += match_result.get('new_companies_fetched', 0)
                        result['new_officers_fetched'] += match_result.get('new_officers_fetched', 0)
                        result['new_appointments_created'] += match_result.get('new_appointments_created', 0)
                        
                    except Exception as e:
                        logger.error(f"Error processing match {match.company_number}: {str(e)}")
                        continue
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Error in _process_single_applicant: {str(e)}")
            raise
        
        return result
    
    def _create_applicant_record(self, session, applicant_data: Dict[str, Any]):
        """Create or get applicant and planning application records"""
        from models import PlanningApplication, Applicant
        
        # Create or get planning application
        planning_app = session.query(PlanningApplication).filter(
            PlanningApplication.reference == applicant_data['planning_reference'],
            PlanningApplication.borough == applicant_data.get('borough', '')
        ).first()
        
        if not planning_app:
            planning_app = PlanningApplication(
                borough=applicant_data.get('borough', ''),
                reference=applicant_data['planning_reference'],
                description=applicant_data.get('description', ''),
                raw_data=applicant_data
            )
            session.add(planning_app)
            session.flush()
        
        # Create or get applicant
        applicant = session.query(Applicant).filter(
            Applicant.planning_application_id == planning_app.id,
            Applicant.normalized_name == applicant_data['normalized_name']
        ).first()
        
        if not applicant:
            applicant = Applicant(
                planning_application_id=planning_app.id,
                raw_name=applicant_data['raw_name'],
                normalized_name=applicant_data['normalized_name'],
                applicant_type=applicant_data['applicant_type'],
                contact_email=applicant_data.get('contact_email'),
                contact_phone=applicant_data.get('contact_phone'),
                contact_address=applicant_data.get('contact_address')
            )
            session.add(applicant)
            session.flush()
        
        return applicant
    
    def _search_potential_companies(self, applicant_name: str) -> List[Dict]:
        """Search for potential company matches using Companies House API"""
        try:
            # Search for companies with similar names
            search_results = self.companies_house.search_companies(
                query=applicant_name, 
                items_per_page=20
            )
            
            # Format results for matching
            formatted_results = []
            for company in search_results:
                formatted_results.append({
                    'id': None,  # Will be set after saving to DB
                    'company_number': company.get('company_number', ''),
                    'company_name': company.get('title', company.get('company_name', '')),
                    'company_status': company.get('company_status', ''),
                    'date_of_creation': company.get('date_of_creation', '')
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching for companies: {str(e)}")
            return []
    
    def _process_company_match(self, session, applicant: Applicant, match: CompanyMatch) -> Dict[str, int]:
        """Process a company match: fetch details, officers, create records"""
        result = {
            'new_companies_fetched': 0,
            'new_officers_fetched': 0,
            'new_appointments_created': 0
        }
        
        try:
            # Step 1: Get or create company record
            company = session.query(Company).filter(
                Company.company_number == match.company_number
            ).first()
            
            if not company:
                # Fetch company details from Companies House
                company_data = self.companies_house.get_company_details(match.company_number)
                if company_data:
                    company_id = self.db_manager.save_company(company_data)
                    company = session.query(Company).filter(Company.id == company_id).first()
                    result['new_companies_fetched'] = 1
                    logger.info(f"Fetched new company: {match.company_number}")
                else:
                    logger.warning(f"Could not fetch company details for: {match.company_number}")
                    return result
            
            # Step 2: Create applicant-company match record
            existing_match = session.query(ApplicantCompanyMatch).filter(
                ApplicantCompanyMatch.applicant_id == applicant.id,
                ApplicantCompanyMatch.company_id == company.id
            ).first()
            
            if not existing_match:
                applicant_match = ApplicantCompanyMatch(
                    applicant_id=applicant.id,
                    company_id=company.id,
                    match_method=match.match_method,
                    confidence_score=match.confidence_score,
                    verified=False
                )
                session.add(applicant_match)
                session.flush()
            
            # Step 3: Fetch and process officers
            officers_data = self.companies_house.get_company_officers(match.company_number)
            
            for officer_data in officers_data:
                try:
                    # Save officer
                    officer_id = self.db_manager.save_officer(officer_data)
                    if officer_id:
                        result['new_officers_fetched'] += 1
                    
                    # Create appointment
                    appointment_data = {
                        'officer_id': officer_id,
                        'company_id': company.id,
                        'officer_role': officer_data.get('officer_role', ''),
                        'appointed_on': officer_data.get('appointed_on'),
                        'resigned_on': officer_data.get('resigned_on')
                    }
                    
                    appointment_id = self.db_manager.save_appointment(appointment_data)
                    if appointment_id:
                        result['new_appointments_created'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing officer for {match.company_number}: {str(e)}")
                    continue
            
            logger.info(f"Processed company {match.company_number}: {result['new_officers_fetched']} officers, {result['new_appointments_created']} appointments")
            
        except Exception as e:
            logger.error(f"Error in _process_company_match: {str(e)}")
            raise
        
        return result
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and statistics"""
        network_stats = self.db_manager.get_officer_network_stats()
        db_stats = self.db_manager.get_database_stats()
        
        return {
            'database_stats': db_stats,
            'network_stats': network_stats,
            'pipeline_config': {
                'batch_size': self.batch_size,
                'min_confidence_score': self.min_confidence_score,
                'max_matches_per_applicant': self.max_matches_per_applicant
            },
            'api_status': {
                'companies_house_configured': bool(self.companies_house.api_key)
            }
        }
    
    def test_pipeline_connectivity(self) -> Dict[str, bool]:
        """Test pipeline connectivity and dependencies"""
        results = {}
        
        # Test database connection
        try:
            with self.db_manager.get_session() as session:
                session.execute('SELECT 1')
            results['database'] = True
        except Exception as e:
            logger.error(f"Database test failed: {str(e)}")
            results['database'] = False
        
        # Test Companies House API
        try:
            if self.companies_house.api_key:
                # Try a simple search
                test_results = self.companies_house.search_companies("Test Ltd", items_per_page=1)
                results['companies_house_api'] = True
            else:
                results['companies_house_api'] = False
        except Exception as e:
            logger.error(f"Companies House API test failed: {str(e)}")
            results['companies_house_api'] = False
        
        # Test applicant processor
        try:
            test_data = {
                'planning_reference': 'TEST/2025/001',
                'applicant_name': 'Test Company Ltd'
            }
            is_valid, _ = self.applicant_processor.validate_applicant_data(test_data)
            results['applicant_processor'] = is_valid
        except Exception as e:
            logger.error(f"Applicant processor test failed: {str(e)}")
            results['applicant_processor'] = False
        
        return results
    
    def _run_contact_enrichment_batch(self, pipeline_stats: Dict[str, Any]) -> Dict[str, int]:
        """Run contact enrichment for companies processed in the current pipeline batch"""
        enrichment_results = {
            'companies_enriched': 0,
            'linkedin_profiles_found': 0,
            'emails_discovered': 0,
            'contacts_created': 0
        }
        
        if not self.contact_enrichment:
            return enrichment_results
        
        try:
            # Get recently processed companies (companies with recent appointments)
            recent_company_ids = self._get_recently_processed_companies()
            
            if not recent_company_ids:
                logger.info("No companies to enrich in this batch")
                return enrichment_results
            
            logger.info(f"Starting contact enrichment for {len(recent_company_ids)} companies")
            
            # Run batch contact enrichment
            batch_results = self.contact_enrichment.batch_enrich_companies(recent_company_ids)
            
            enrichment_results['companies_enriched'] = batch_results.get('companies_processed', 0)
            enrichment_results['linkedin_profiles_found'] = batch_results.get('total_linkedin_profiles', 0)
            enrichment_results['emails_discovered'] = batch_results.get('total_emails_discovered', 0)
            enrichment_results['contacts_created'] = batch_results.get('total_contacts_created', 0)
            
            # Log any failed companies
            if batch_results.get('failed_companies'):
                for failed in batch_results['failed_companies']:
                    error_msg = f"Enrichment failed for company {failed['company_id']}: {failed.get('errors', 'Unknown error')}"
                    pipeline_stats['errors'].append(error_msg)
            
        except Exception as e:
            logger.error(f"Batch contact enrichment error: {str(e)}")
            pipeline_stats['errors'].append(f"Batch contact enrichment failed: {str(e)}")
        
        return enrichment_results
    
    def _get_recently_processed_companies(self, hours_ago: int = 1) -> List[int]:
        """Get company IDs that have been recently processed (have recent appointments)"""
        with self.db_manager.get_session() as session:
            from datetime import datetime, timedelta
            
            cutoff_time = datetime.now() - timedelta(hours=hours_ago)
            
            # Get companies that have had appointments created in the last hour
            recent_appointments = session.query(Appointment.company_id).filter(
                Appointment.created_at >= cutoff_time
            ).distinct().all()
            
            company_ids = [appointment.company_id for appointment in recent_appointments]
            
            # Also include companies that were created recently
            recent_companies = session.query(Company.id).filter(
                Company.created_at >= cutoff_time
            ).all()
            
            company_ids.extend([company.id for company in recent_companies])
            
            # Remove duplicates and return
            return list(set(company_ids))