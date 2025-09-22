"""
Contact Enrichment Pipeline for discovering LinkedIn profiles and email addresses.
Integrates with BrightData LinkedIn API and Hunter.io for comprehensive contact enrichment.
"""
import os
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import DatabaseManager
from api_clients import BrightDataClient, HunterClient
from models import Contact, Company, Officer, Appointment

logger = logging.getLogger(__name__)

class ContactEnrichmentPipeline:
    """
    Comprehensive contact enrichment pipeline that discovers LinkedIn profiles and email addresses
    for company officers using BrightData and Hunter.io APIs.
    """
    
    def __init__(self, db_manager: DatabaseManager, brightdata_key: str = None, hunter_key: str = None):
        self.db_manager = db_manager
        
        # Initialize API clients
        self.brightdata_key = brightdata_key or os.getenv("BRIGHTDATA_API_KEY")
        self.hunter_key = hunter_key or os.getenv("HUNTER_API_KEY")
        
        self.brightdata_client = BrightDataClient(self.brightdata_key) if self.brightdata_key else None
        self.hunter_client = HunterClient(self.hunter_key) if self.hunter_key else None
        
        # Pipeline configuration
        self.max_workers = 3  # Concurrent workers for API calls
        self.rate_limit_delay = 1.0  # Delay between API calls
        self.confidence_threshold = 0.6  # Minimum confidence score for auto-approval
        
        logger.info("Contact enrichment pipeline initialized")
    
    def enrich_company_contacts(self, company_id: int) -> Dict[str, Any]:
        """
        Enrich all contacts for a specific company by discovering LinkedIn profiles 
        and email addresses for its officers.
        """
        enrichment_stats = {
            'company_id': company_id,
            'officers_processed': 0,
            'linkedin_profiles_found': 0,
            'emails_discovered': 0,
            'existing_contacts_updated': 0,
            'new_contacts_created': 0,
            'errors': []
        }
        
        try:
            with self.db_manager.get_session() as session:
                # Get company and its officers
                company = session.query(Company).filter(Company.id == company_id).first()
                if not company:
                    raise ValueError(f"Company with ID {company_id} not found")
                
                # Get active officers through appointments
                active_appointments = session.query(Appointment).filter(
                    Appointment.company_id == company_id,
                    Appointment.is_active == True
                ).all()
                
                officers = [appointment.officer for appointment in active_appointments if appointment.officer]
                
                if not officers:
                    logger.info(f"No active officers found for company {company.company_name}")
                    return enrichment_stats
                
                logger.info(f"Starting enrichment for {len(officers)} officers at {company.company_name}")
                
                # Discover company domain first (needed for email patterns)
                company_domain = self._discover_company_domain(company)
                
                # Process each officer
                for officer in officers:
                    try:
                        officer_result = self._enrich_officer_contacts(
                            officer, company, company_domain
                        )
                        
                        enrichment_stats['officers_processed'] += 1
                        enrichment_stats['linkedin_profiles_found'] += officer_result.get('linkedin_found', 0)
                        enrichment_stats['emails_discovered'] += officer_result.get('emails_found', 0)
                        enrichment_stats['existing_contacts_updated'] += officer_result.get('contacts_updated', 0)
                        enrichment_stats['new_contacts_created'] += officer_result.get('contacts_created', 0)
                        
                        # Rate limiting
                        time.sleep(self.rate_limit_delay)
                        
                    except Exception as e:
                        error_msg = f"Failed to enrich contacts for officer {officer.name}: {str(e)}"
                        enrichment_stats['errors'].append(error_msg)
                        logger.error(error_msg)
                
                logger.info(f"Enrichment completed for company {company.company_name}. "
                          f"Found {enrichment_stats['linkedin_profiles_found']} LinkedIn profiles, "
                          f"{enrichment_stats['emails_discovered']} email addresses")
                
        except Exception as e:
            error_msg = f"Company enrichment failed: {str(e)}"
            enrichment_stats['errors'].append(error_msg)
            logger.error(error_msg)
        
        return enrichment_stats
    
    def _enrich_officer_contacts(self, officer: Officer, company: Company, 
                               company_domain: Optional[str] = None) -> Dict[str, int]:
        """Enrich contacts for a specific officer"""
        result = {
            'linkedin_found': 0,
            'emails_found': 0,
            'contacts_updated': 0,
            'contacts_created': 0
        }
        
        # Parse officer name
        first_name, last_name = self._parse_officer_name(officer.name)
        if not first_name or not last_name:
            logger.warning(f"Could not parse name: {officer.name}")
            return result
        
        # 1. LinkedIn profile discovery
        if self.brightdata_client:
            try:
                linkedin_url = self._discover_linkedin_profile(
                    first_name, last_name, company.company_name
                )
                
                if linkedin_url:
                    contact_result = self.db_manager.upsert_contact(
                        officer_id=officer.id,
                        contact_type='linkedin',
                        contact_value=linkedin_url,
                        source='brightdata_linkedin',
                        confidence_score=self._calculate_linkedin_confidence(
                            officer.name, company.company_name, linkedin_url
                        )
                    )
                    
                    if contact_result['created']:
                        result['contacts_created'] += 1
                        result['linkedin_found'] += 1
                    else:
                        result['contacts_updated'] += 1
                    
                    logger.info(f"LinkedIn profile found for {officer.name}: {linkedin_url}")
                    
            except Exception as e:
                logger.warning(f"LinkedIn search failed for {officer.name}: {str(e)}")
        
        # 2. Email discovery
        if self.hunter_client and company_domain:
            try:
                email_candidates = self._discover_officer_emails(
                    first_name, last_name, company_domain
                )
                
                for email_data in email_candidates:
                    contact_result = self.db_manager.upsert_contact(
                        officer_id=officer.id,
                        contact_type='email',
                        contact_value=email_data['email'],
                        source='hunter_email',
                        confidence_score=email_data['confidence'],
                        verification_status=email_data['verification_status']
                    )
                    
                    if contact_result['created']:
                        result['contacts_created'] += 1
                        result['emails_found'] += 1
                    else:
                        result['contacts_updated'] += 1
                    
                    logger.info(f"Email found for {officer.name}: {email_data['email']} "
                              f"(confidence: {email_data['confidence']:.2f})")
                    
            except Exception as e:
                logger.warning(f"Email search failed for {officer.name}: {str(e)}")
        
        return result
    
    def _discover_company_domain(self, company: Company) -> Optional[str]:
        """Discover company domain using Hunter.io"""
        if not self.hunter_client:
            return None
        
        try:
            domain = self.hunter_client.find_company_domain(company.company_name)
            
            if domain:
                # Store domain as company contact
                self.db_manager.upsert_contact(
                    company_id=company.id,
                    contact_type='domain',
                    contact_value=domain,
                    source='hunter_domain',
                    confidence_score=0.8
                )
                logger.info(f"Domain discovered for {company.company_name}: {domain}")
            
            return domain
            
        except Exception as e:
            logger.warning(f"Domain discovery failed for {company.company_name}: {str(e)}")
            return None
    
    def _discover_linkedin_profile(self, first_name: str, last_name: str, 
                                 company_name: str) -> Optional[str]:
        """Discover LinkedIn profile using BrightData"""
        try:
            linkedin_url = self.brightdata_client.search_linkedin_profile(
                first_name, last_name, company_name
            )
            return linkedin_url
        except Exception as e:
            logger.warning(f"LinkedIn discovery failed: {str(e)}")
            return None
    
    def _discover_officer_emails(self, first_name: str, last_name: str, 
                               domain: str) -> List[Dict[str, Any]]:
        """Discover officer email addresses using common patterns"""
        if not self.hunter_client:
            return []
        
        # Common email patterns to try
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}@{domain}",
            f"{first_name.lower()}{last_name.lower()}@{domain}",
            f"{first_name[0].lower()}.{last_name.lower()}@{domain}",
            f"{first_name[0].lower()}{last_name.lower()}@{domain}",
            f"{first_name.lower()}@{domain}",
            f"{last_name.lower()}@{domain}"
        ]
        
        discovered_emails = []
        
        for pattern in patterns:
            try:
                # Use Hunter.io email finder
                email_result = self.hunter_client.verify_email(pattern)
                
                if email_result and email_result.get('deliverable') != 'undeliverable':
                    confidence = self._calculate_email_confidence(email_result)
                    verification_status = self._map_hunter_status(email_result)
                    
                    discovered_emails.append({
                        'email': pattern,
                        'confidence': confidence,
                        'verification_status': verification_status,
                        'hunter_data': email_result
                    })
                    
                    # Stop after finding first valid email to avoid spamming
                    if confidence > 0.7:
                        break
                    
            except Exception as e:
                logger.debug(f"Email verification failed for {pattern}: {str(e)}")
                continue
        
        return discovered_emails
    
    def _parse_officer_name(self, full_name: str) -> Tuple[str, str]:
        """Parse officer name into first and last name"""
        if not full_name:
            return "", ""
        
        # Remove titles and clean name
        cleaned = re.sub(r'\b(MR|MRS|MS|MISS|DR|PROF)\b\.?\s*', '', full_name.upper())
        cleaned = re.sub(r'\s+', ' ', cleaned.strip())
        
        parts = cleaned.split()
        if len(parts) < 2:
            return "", ""
        
        # Take first name and last name, ignore middle names
        first_name = parts[0].title()
        last_name = parts[-1].title()
        
        return first_name, last_name
    
    def _calculate_linkedin_confidence(self, officer_name: str, company_name: str, 
                                     linkedin_url: str) -> float:
        """Calculate confidence score for LinkedIn profile match"""
        base_confidence = 0.6  # Base confidence for BrightData results
        
        # Add points for name matching in URL
        officer_lower = officer_name.lower().replace(' ', '')
        if any(part.lower() in linkedin_url.lower() for part in officer_name.split()):
            base_confidence += 0.2
        
        # Add points for company matching
        company_clean = re.sub(r'[^\w\s]', '', company_name.lower())
        if any(word in linkedin_url.lower() for word in company_clean.split()):
            base_confidence += 0.1
        
        return min(base_confidence, 1.0)
    
    def _calculate_email_confidence(self, hunter_result: Dict) -> float:
        """Calculate confidence score from Hunter.io email verification"""
        if not hunter_result:
            return 0.0
        
        score = hunter_result.get('score', 0) / 100.0  # Hunter scores are 0-100
        
        # Boost confidence based on result quality
        result = hunter_result.get('result', '').lower()
        if result == 'deliverable':
            score = max(score, 0.8)
        elif result == 'risky':
            score = max(score, 0.5)
        elif result == 'undeliverable':
            score = min(score, 0.2)
        
        return min(score, 1.0)
    
    def _map_hunter_status(self, hunter_result: Dict) -> str:
        """Map Hunter.io verification result to our verification status"""
        if not hunter_result:
            return 'unverified'
        
        result = hunter_result.get('result', '').lower()
        if result == 'deliverable':
            return 'verified'
        elif result == 'risky':
            return 'risky'
        elif result == 'undeliverable':
            return 'invalid'
        else:
            return 'unverified'
    
    def batch_enrich_companies(self, company_ids: List[int]) -> Dict[str, Any]:
        """Batch enrich multiple companies with rate limiting and error handling"""
        batch_stats = {
            'total_companies': len(company_ids),
            'companies_processed': 0,
            'total_linkedin_profiles': 0,
            'total_emails_discovered': 0,
            'total_contacts_created': 0,
            'failed_companies': [],
            'processing_time': 0
        }
        
        start_time = time.time()
        
        logger.info(f"Starting batch enrichment for {len(company_ids)} companies")
        
        for company_id in company_ids:
            try:
                result = self.enrich_company_contacts(company_id)
                
                batch_stats['companies_processed'] += 1
                batch_stats['total_linkedin_profiles'] += result.get('linkedin_profiles_found', 0)
                batch_stats['total_emails_discovered'] += result.get('emails_discovered', 0)
                batch_stats['total_contacts_created'] += result.get('new_contacts_created', 0)
                
                if result.get('errors'):
                    batch_stats['failed_companies'].append({
                        'company_id': company_id,
                        'errors': result['errors']
                    })
                
            except Exception as e:
                error_msg = f"Batch enrichment failed for company {company_id}: {str(e)}"
                batch_stats['failed_companies'].append({
                    'company_id': company_id,
                    'errors': [error_msg]
                })
                logger.error(error_msg)
        
        batch_stats['processing_time'] = time.time() - start_time
        
        logger.info(f"Batch enrichment completed in {batch_stats['processing_time']:.2f} seconds. "
                   f"Found {batch_stats['total_linkedin_profiles']} LinkedIn profiles, "
                   f"{batch_stats['total_emails_discovered']} email addresses")
        
        return batch_stats


class EnhancedHunterClient(HunterClient):
    """Enhanced Hunter.io client with email verification and search capabilities"""
    
    def verify_email(self, email: str) -> Optional[Dict]:
        """Verify a specific email address using Hunter.io"""
        if not self.api_key or not email:
            return None
        
        try:
            params = {
                'email': email,
                'api_key': self.api_key
            }
            
            response = self.session.get(
                f"{self.base_url}/v2/email-verifier",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('data', {})
            
            return None
            
        except Exception as e:
            logger.debug(f"Email verification failed for {email}: {str(e)}")
            return None
    
    def find_emails_by_domain(self, domain: str, first_name: str = None, 
                             last_name: str = None, limit: int = 10) -> List[Dict]:
        """Find email addresses for a domain, optionally filtered by name"""
        if not self.api_key or not domain:
            return []
        
        try:
            params = {
                'domain': domain,
                'api_key': self.api_key,
                'limit': limit
            }
            
            if first_name:
                params['first_name'] = first_name
            if last_name:
                params['last_name'] = last_name
            
            response = self.session.get(
                f"{self.base_url}/v2/domain-search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                emails = data.get('data', {}).get('emails', [])
                return emails
            
            return []
            
        except Exception as e:
            logger.debug(f"Domain email search failed for {domain}: {str(e)}")
            return []