"""
Weekly Automation Scheduler for Planning Application Pipeline Processing.

Comprehensive scheduler that:
1. Discovers new planning applications across London boroughs
2. Processes them through the complete intelligence pipeline
3. Tracks execution statistics and handles errors
4. Provides monitoring and alerting capabilities
"""
import os
import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler import events

from database import DatabaseManager
from applicant_pipeline import ApplicantPipeline
from contact_enrichment import ContactEnrichmentPipeline
from api_clients import LondonPlanningClient, CompaniesHouseClient
from models import AutomationRun, AutomationConfig, AutomationSchedule
from automation_monitoring import AutomationMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeeklyAutomationScheduler:
    """
    Main automation scheduler that orchestrates the complete planning application pipeline.
    Runs weekly on Sundays at 2 AM by default, with configurable settings.
    """
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.db_manager = DatabaseManager(self.database_url)
        
        # Initialize API clients
        companies_house_key = os.getenv("COMPANIES_HOUSE_API_KEY")
        brightdata_key = os.getenv("BRIGHTDATA_API_KEY")
        hunter_key = os.getenv("HUNTER_API_KEY")
        
        if not companies_house_key:
            raise ValueError("COMPANIES_HOUSE_API_KEY environment variable is required")
        
        self.london_planning = LondonPlanningClient()
        self.applicant_pipeline = ApplicantPipeline(
            self.db_manager, companies_house_key, brightdata_key, hunter_key
        )
        
        # Initialize monitoring system
        self.monitor = AutomationMonitor(self.db_manager)
        
        # Scheduler configuration
        self.scheduler = None
        self.job_defaults = {
            'coalesce': False,
            'max_instances': 1,  # Prevent overlapping runs
            'misfire_grace_time': 3600  # 1 hour grace period
        }
        
        # Default configuration
        self.default_config = {
            'schedule_enabled': True,
            'schedule_cron': '0 2 * * 0',  # Sundays at 2 AM
            'boroughs_to_process': [
                'Westminster', 'Camden', 'Islington', 'Hackney', 'Tower Hamlets',
                'Southwark', 'Lambeth', 'Brent', 'Ealing', 'Barnet'
            ],
            'days_back_to_search': 7,  # Look back 7 days for new applications
            'batch_size': 50,  # Process applications in batches
            'enable_contact_enrichment': True,
            'application_types': ['outline', 'major'],  # Focus on major developments
            'rate_limit_delay': 1.0,  # Delay between API calls
            'max_retry_attempts': 3,
            'email_alerts_enabled': False,
            'alert_email': os.getenv("AUTOMATION_ALERT_EMAIL", ""),
            
            # Companies House weekly automation
            'companies_house_enabled': True,
            'companies_house_cron': '0 9 * * 1',  # Mondays at 9 AM
            'sic_code': '41100',  # Construction SIC code
            'incorporation_lookback_days': 7,  # Check previous week
        }
        
        logger.info("Weekly automation scheduler initialized")
    
    def initialize_scheduler(self):
        """Initialize APScheduler with job persistence and single-instance guarding"""
        try:
            # Configure job store (using memory store to avoid timezone pickle issues with persistence)
            # Note: Jobs will not survive application restarts - handled by startup re-scheduling
            jobstores = {
                'default': MemoryJobStore()  # In-memory store - jobs recreated on startup
            }
            
            # Configure executors
            executors = {
                'default': APSThreadPoolExecutor(10),  # 10 worker threads
            }
            
            # Create scheduler without timezone parameter to avoid pickle issues
            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=self.job_defaults
                # No timezone parameter - let APScheduler handle this internally
            )
            
            # Add event listeners for monitoring
            self.scheduler.add_listener(
                self._on_job_executed, 
                events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR
            )
            self.scheduler.add_listener(
                self._on_job_submitted,
                events.EVENT_JOB_SUBMITTED
            )
            
            # Initialize default configuration
            self._initialize_default_config()
            
            logger.info("✅ APScheduler initialized with in-memory job storage (non-persistent)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize scheduler: {str(e)}")
            return False
    
    def start_scheduler(self):
        """Start the scheduler and add weekly job with crash recovery"""
        if not self.scheduler:
            if not self.initialize_scheduler():
                return False
        
        try:
            self.scheduler.start()
            
            # Crash recovery: re-schedule jobs since we use in-memory storage
            logger.info("🔄 Performing crash recovery - re-scheduling jobs after restart")
            
            # Add weekly automation job if enabled
            if self._get_config('schedule_enabled', True):
                self._schedule_weekly_job()
                
            # Add Companies House weekly automation job if enabled
            if self._get_config('companies_house_enabled', True):
                self._schedule_companies_house_job()
                
            # Check for any interrupted automation runs and handle recovery
            self._handle_crash_recovery()
            
            logger.info("🚀 Weekly automation scheduler started with crash recovery completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start scheduler: {str(e)}")
            return False
    
    def stop_scheduler(self):
        """Stop the scheduler gracefully"""
        if self.scheduler:
            try:
                self.scheduler.shutdown(wait=True)
                logger.info("🛑 Scheduler stopped gracefully")
            except Exception as e:
                logger.error(f"❌ Error stopping scheduler: {str(e)}")
    
    def _schedule_weekly_job(self):
        """Schedule the weekly automation job"""
        cron_expression = self._get_config('schedule_cron', '0 2 * * 0')
        
        # Parse cron expression (minute hour day month day_of_week)
        parts = cron_expression.split()
        if len(parts) != 5:
            logger.error(f"Invalid cron expression: {cron_expression}")
            return False
        
        minute, hour, day, month, day_of_week = parts
        
        # Add job to scheduler
        self.scheduler.add_job(
            func=self.run_weekly_automation,
            trigger=CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week
            ),
            id='weekly_planning_automation',
            name='Weekly Planning Application Processing',
            replace_existing=True
        )
        
        # Calculate next run time
        next_run = self.scheduler.get_job('weekly_planning_automation').next_run_time
        logger.info(f"📅 Weekly automation scheduled for: {next_run}")
        
        # Save schedule to database
        self._save_schedule_config('weekly_planning_automation', {
            'cron_expression': cron_expression,
            'job_type': 'weekly_automation',
            'enabled': True
        })
        
        return True
    
    def _schedule_companies_house_job(self):
        """Schedule the Companies House weekly automation job"""
        cron_expression = self._get_config('companies_house_cron', '0 9 * * 1')
        
        # Parse cron expression (minute hour day month day_of_week)
        parts = cron_expression.split()
        if len(parts) != 5:
            logger.error(f"Invalid Companies House cron expression: {cron_expression}")
            return False
        
        minute, hour, day, month, day_of_week = parts
        
        # Add job to scheduler
        self.scheduler.add_job(
            func=self.run_companies_house_automation,
            trigger=CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week
            ),
            id='weekly_companies_house_automation',
            name='Weekly Companies House SIC 41100 Processing',
            replace_existing=True
        )
        
        # Calculate next run time
        next_run = self.scheduler.get_job('weekly_companies_house_automation').next_run_time
        logger.info(f"📅 Companies House automation scheduled for: {next_run}")
        
        return True
    
    def run_weekly_automation(self, run_type: str = 'weekly_scheduled'):
        """
        Main automation function that processes new planning applications.
        This is called by the scheduler or manually triggered.
        """
        run_id = None
        start_time = datetime.now()
        
        try:
            logger.info("🚀 Starting weekly planning application automation")
            
            # Create automation run record using monitor
            run_id = self.monitor.log_automation_start(run_type, {
                'schedule_type': 'weekly',
                'trigger': 'scheduled' if run_type == 'weekly_scheduled' else 'manual'
            })
            
            # Get configuration
            boroughs = self._get_config('boroughs_to_process', self.default_config['boroughs_to_process'])
            days_back = self._get_config('days_back_to_search', 7)
            batch_size = self._get_config('batch_size', 50)
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"🏛️ Processing {len(boroughs)} boroughs from {start_date.date()} to {end_date.date()}")
            
            # Step 1: Discover new planning applications
            all_applications = []
            discovery_stats = {
                'boroughs_processed': 0,
                'applications_discovered': 0,
                'errors': []
            }
            
            for borough in boroughs:
                try:
                    logger.info(f"🔍 Discovering applications in {borough}")
                    
                    # Search for applications in this borough
                    applications = self.london_planning.search_planning_applications(
                        local_authority=borough,
                        start_date=start_date.strftime('%Y-%m-%d'),
                        limit=1000,  # Large limit to catch all new applications
                        enable_outline_filter=True  # Focus on major developments
                    )
                    
                    # Filter for new applications not already processed
                    new_applications = self._filter_new_applications(applications, borough)
                    
                    all_applications.extend(new_applications)
                    discovery_stats['applications_discovered'] += len(new_applications)
                    discovery_stats['boroughs_processed'] += 1
                    
                    logger.info(f"✅ {borough}: Found {len(new_applications)} new applications")
                    
                    # Rate limiting
                    time.sleep(self._get_config('rate_limit_delay', 1.0))
                    
                except Exception as e:
                    error_msg = f"Failed to process {borough}: {str(e)}"
                    discovery_stats['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
            
            # Update run statistics
            self._update_run_stats(run_id, {
                'applications_discovered': discovery_stats['applications_discovered'],
                'boroughs_processed': discovery_stats['boroughs_processed'],
                'error_count': len(discovery_stats['errors']),
                'error_details': '; '.join(discovery_stats['errors'][:5])  # Limit error details
            })
            
            logger.info(f"📊 Discovery complete: {discovery_stats['applications_discovered']} applications from {discovery_stats['boroughs_processed']} boroughs")
            
            if not all_applications:
                logger.info("ℹ️ No new applications found - automation complete")
                self._complete_automation_run(run_id, 'completed', start_time)
                return True
            
            # Step 2: Process applications through pipeline
            logger.info(f"⚙️ Processing {len(all_applications)} applications through pipeline")
            
            # Process in batches to manage memory and API limits
            total_processed = 0
            pipeline_stats = {
                'applications_processed': 0,
                'companies_matched': 0,
                'new_companies_created': 0,
                'new_officers_created': 0,
                'contacts_enriched': 0,
                'errors': []
            }
            
            for i in range(0, len(all_applications), batch_size):
                batch = all_applications[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(all_applications) + batch_size - 1) // batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} applications)")
                
                try:
                    # Convert applications to applicants
                    applicants = self._extract_applicants_from_applications(batch)
                    
                    if applicants:
                        # Process through applicant pipeline
                        batch_result = self.applicant_pipeline.process_applicant_batch(applicants)
                        
                        # Aggregate statistics
                        pipeline_stats['applications_processed'] += batch_result.get('processed_applicants', 0)
                        pipeline_stats['companies_matched'] += batch_result.get('matched_companies', 0)
                        pipeline_stats['new_companies_created'] += batch_result.get('new_companies_fetched', 0)
                        pipeline_stats['new_officers_created'] += batch_result.get('new_officers_fetched', 0)
                        pipeline_stats['contacts_enriched'] += batch_result.get('companies_enriched', 0)
                        pipeline_stats['errors'].extend(batch_result.get('errors', []))
                        
                        total_processed += len(batch)
                        
                        logger.info(f"✅ Batch {batch_num} completed: {batch_result.get('processed_applicants', 0)} applicants processed")
                    
                except Exception as e:
                    error_msg = f"Batch {batch_num} failed: {str(e)}"
                    pipeline_stats['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
                
                # Rate limiting between batches
                time.sleep(self._get_config('rate_limit_delay', 1.0))
            
            # Update final run statistics
            self._update_run_stats(run_id, {
                'applications_processed': pipeline_stats['applications_processed'],
                'companies_matched': pipeline_stats['companies_matched'],
                'new_companies_created': pipeline_stats['new_companies_created'],
                'new_officers_created': pipeline_stats['new_officers_created'],
                'contacts_enriched': pipeline_stats['contacts_enriched'],
                'error_count': len(pipeline_stats['errors']),
                'error_details': '; '.join(pipeline_stats['errors'][:10]),  # Limit error details
                'date_range_start': start_date,
                'date_range_end': end_date,
                'boroughs_processed': boroughs
            })
            
            # Complete the run
            status = 'completed' if len(pipeline_stats['errors']) == 0 else 'partial'
            self._complete_automation_run(run_id, status, start_time)
            
            logger.info(f"🎉 Weekly automation completed successfully!")
            logger.info(f"📈 Statistics: {pipeline_stats['applications_processed']} processed, "
                       f"{pipeline_stats['companies_matched']} companies matched, "
                       f"{pipeline_stats['contacts_enriched']} contacts enriched")
            
            # Send alerts if configured
            if self._get_config('email_alerts_enabled', False):
                self._send_completion_alert(run_id, pipeline_stats)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Weekly automation failed: {str(e)}")
            
            if run_id:
                self._update_run_stats(run_id, {
                    'error_count': 1,
                    'error_details': str(e)
                })
                self._complete_automation_run(run_id, 'failed', start_time)
            
            # Send failure alert
            if self._get_config('email_alerts_enabled', False):
                self._send_failure_alert(str(e))
            
            return False
    
    def run_companies_house_automation(self, run_type: str = 'companies_house_scheduled'):
        """
        Weekly Companies House automation that searches for new SIC 41100 companies 
        incorporated in the previous week and enriches them with charge and officer data.
        """
        run_id = None
        start_time = datetime.now()
        
        try:
            logger.info("🏢 Starting weekly Companies House SIC 41100 automation")
            
            # Create automation run record
            run_id = self.monitor.log_automation_start(run_type, {
                'schedule_type': 'companies_house_weekly',
                'trigger': 'scheduled' if run_type == 'companies_house_scheduled' else 'manual',
                'sic_code': self._get_config('sic_code', '41100')
            })
            
            # Initialize Companies House client
            companies_house_key = os.getenv("COMPANIES_HOUSE_API_KEY")
            if not companies_house_key:
                raise ValueError("COMPANIES_HOUSE_API_KEY not configured")
            
            from api_clients import CompaniesHouseClient
            ch_client = CompaniesHouseClient(companies_house_key)
            
            # Calculate date range - USER REQUESTED: from 15/09/2025 onwards for full capture
            if run_type.startswith('manual'):
                # Manual runs: capture from 15/09/2025 onwards as requested by user
                previous_monday = datetime(2025, 9, 15)
                current_monday = datetime.now()
            else:
                # Scheduled runs: previous Monday to current Monday
                today = datetime.now()
                days_since_monday = today.weekday()  # 0 = Monday, 6 = Sunday
                current_monday = today - timedelta(days=days_since_monday)
                previous_monday = current_monday - timedelta(days=7)
            
            logger.info(f"🗓️ Searching for companies incorporated from {previous_monday.date()} to {current_monday.date()}")
            
            # ARCHITECT FIX: Use Advanced Search endpoint with proper date range and full pagination
            sic_code = self._get_config('sic_code', '41100')
            
            # Use proper date range: from 15/09/2025 to capture all 258 companies
            start_date = datetime(2025, 9, 15) if run_type.startswith('manual') else previous_monday
            end_date = datetime.now()
            
            logger.info(f"🔍 Using Advanced Search: SIC {sic_code}, {start_date.date()} to {end_date.date()}")
            
            companies = ch_client.search_companies_by_incorporation_date(
                date_from=start_date,
                date_to=end_date,
                location_filter="",
                items_per_page=100,
                sic_codes=[sic_code],  # Filter SIC 41100 directly in API
                max_results=5000  # Increased to ensure we get all 258+ companies
            )
            
            logger.info(f"🔍 Found {len(companies)} companies incorporated in the past week")
            
            # Companies are already filtered by SIC 41100 from the API call
            sic_41100_companies = companies
            
            logger.info(f"🏗️ Found {len(sic_41100_companies)} new SIC 41100 (construction) companies")
            
            if not sic_41100_companies:
                logger.info("ℹ️ No new SIC 41100 companies found this week")
                self._complete_automation_run(run_id, 'completed', start_time)
                return True
            
            # Enrich companies with charge and officer data
            enriched_companies = []
            failed_enrichments = 0
            
            for i, company in enumerate(sic_41100_companies):
                try:
                    company_number = company.get('company_number', '')
                    logger.info(f"🔄 Enriching company {i+1}/{len(sic_41100_companies)}: {company_number}")
                    
                    # Get officer information
                    officers = ch_client.get_company_officers(company_number)
                    company['officers'] = officers
                    
                    # Get charge information
                    charges = ch_client.get_company_charges(company_number)
                    company['charges'] = charges
                    
                    # Add to enriched list
                    enriched_companies.append(company)
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ Failed to enrich company {company_number}: {str(e)}")
                    failed_enrichments += 1
                    continue
            
            # Store enriched companies in database
            companies_added = 0
            if enriched_companies:
                companies_added = self._store_companies_house_results(enriched_companies, run_id)
            
            # Update run statistics
            self._update_run_stats(run_id, {
                'companies_found': len(companies),
                'sic_41100_companies': len(sic_41100_companies),
                'companies_enriched': len(enriched_companies),
                'failed_enrichments': failed_enrichments,
                'companies_added_to_db': companies_added,
                'date_range_start': previous_monday,
                'date_range_end': current_monday,
                'sic_code': sic_code
            })
            
            # Complete the run
            status = 'completed' if failed_enrichments == 0 else 'partial'
            self._complete_automation_run(run_id, status, start_time)
            
            logger.info(f"🎉 Companies House automation completed!")
            logger.info(f"📈 Added {companies_added} new SIC 41100 companies to database")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Companies House automation failed: {str(e)}")
            
            if run_id:
                self._update_run_stats(run_id, {
                    'error_count': 1,
                    'error_details': str(e)
                })
                self._complete_automation_run(run_id, 'failed', start_time)
            
            return False
    
    def run_manual_automation(self, boroughs: Optional[List[str]] = None, 
                            days_back: Optional[int] = None):
        """Manually trigger automation with custom parameters"""
        logger.info("🔧 Manual automation triggered")
        
        # Override configuration temporarily
        original_boroughs = self._get_config('boroughs_to_process')
        original_days = self._get_config('days_back_to_search')
        
        try:
            if boroughs:
                self._set_config('boroughs_to_process', boroughs)
            if days_back:
                self._set_config('days_back_to_search', days_back)
            
            # Run automation
            return self.run_weekly_automation('manual')
            
        finally:
            # Restore original configuration
            self._set_config('boroughs_to_process', original_boroughs)
            self._set_config('days_back_to_search', original_days)
    
    def _filter_new_applications(self, applications: List[Dict], borough: str) -> List[Dict]:
        """Filter out applications that have already been processed"""
        if not applications:
            return []
        
        new_applications = []
        
        with self.db_manager.get_session() as session:
            for app in applications:
                reference = app.get('reference', '')
                if reference:
                    # Check if application already exists
                    from models import PlanningApplication
                    existing = session.query(PlanningApplication).filter(
                        PlanningApplication.borough == borough,
                        PlanningApplication.reference == reference
                    ).first()
                    
                    if not existing:
                        # Add borough to application data
                        app['borough'] = borough
                        new_applications.append(app)
        
        return new_applications
    
    def _extract_applicants_from_applications(self, applications: List[Dict]) -> List[Dict]:
        """Extract applicant data from planning applications"""
        applicants = []
        
        for app in applications:
            # Save the planning application first
            try:
                self.db_manager.save_planning_application(app)
            except Exception as e:
                logger.warning(f"Failed to save planning application {app.get('reference', 'Unknown')}: {str(e)}")
            
            # Extract applicant information
            applicant_name = app.get('applicant_name', '')
            if not applicant_name:
                # Try alternative fields
                applicant_name = app.get('name', '') or app.get('organisation', '')
            
            if applicant_name and applicant_name.strip():
                applicant_data = {
                    'raw_name': applicant_name.strip(),
                    'planning_reference': app.get('reference', ''),
                    'borough': app.get('borough', ''),
                    'application_type': app.get('application_type', ''),
                    'description': app.get('description', ''),
                    'address': app.get('address', ''),
                    'planning_url': app.get('planning_url', '')
                }
                applicants.append(applicant_data)
        
        return applicants
    
    def _create_automation_run(self, run_type: str, start_time: datetime) -> int:
        """Create a new automation run record"""
        with self.db_manager.get_session() as session:
            run = AutomationRun(
                run_type=run_type,
                status='running',
                started_at=start_time
            )
            session.add(run)
            session.flush()
            run_id = run.id
            logger.info(f"📝 Created automation run {run_id}")
            return run_id
    
    def _update_run_stats(self, run_id: int, stats: Dict[str, Any]):
        """Update automation run statistics"""
        with self.db_manager.get_session() as session:
            run = session.query(AutomationRun).filter(AutomationRun.id == run_id).first()
            if run:
                for key, value in stats.items():
                    if hasattr(run, key):
                        setattr(run, key, value)
    
    def _complete_automation_run(self, run_id: int, status: str, start_time: datetime):
        """Complete automation run and calculate duration"""
        with self.db_manager.get_session() as session:
            run = session.query(AutomationRun).filter(AutomationRun.id == run_id).first()
            if run:
                run.status = status
                run.completed_at = datetime.now()
                run.duration_seconds = int((run.completed_at - start_time).total_seconds())
                logger.info(f"✅ Automation run {run_id} completed with status: {status}")
    
    def _initialize_default_config(self):
        """Initialize default configuration in database"""
        with self.db_manager.get_session() as session:
            for key, value in self.default_config.items():
                config = session.query(AutomationConfig).filter(
                    AutomationConfig.config_key == key
                ).first()
                
                if not config:
                    config = AutomationConfig(
                        config_key=key,
                        config_value=json.dumps(value),
                        description=f"Default configuration for {key}"
                    )
                    session.add(config)
    
    def _get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value from database"""
        with self.db_manager.get_session() as session:
            config = session.query(AutomationConfig).filter(
                AutomationConfig.config_key == key
            ).first()
            
            if config and config.config_value:
                try:
                    return json.loads(config.config_value)
                except (json.JSONDecodeError, TypeError):
                    return config.config_value
            
            return default
    
    def _set_config(self, key: str, value: Any):
        """Set configuration value in database"""
        with self.db_manager.get_session() as session:
            config = session.query(AutomationConfig).filter(
                AutomationConfig.config_key == key
            ).first()
            
            if not config:
                config = AutomationConfig(config_key=key)
                session.add(config)
            
            config.config_value = json.dumps(value) if not isinstance(value, str) else value
            config.updated_at = datetime.now()
    
    def _save_schedule_config(self, job_id: str, config: Dict[str, Any]):
        """Save schedule configuration to database"""
        with self.db_manager.get_session() as session:
            schedule = session.query(AutomationSchedule).filter(
                AutomationSchedule.job_id == job_id
            ).first()
            
            if not schedule:
                schedule = AutomationSchedule(
                    job_id=job_id,
                    job_name=config.get('job_name', 'Weekly Planning Automation'),
                    schedule_type='cron'
                )
                session.add(schedule)
            
            schedule.cron_expression = config.get('cron_expression')
            schedule.is_enabled = config.get('enabled', True)
            schedule.job_config = config
            schedule.updated_at = datetime.now()
            
            # Update next execution time
            if self.scheduler:
                job = self.scheduler.get_job(job_id)
                if job:
                    schedule.next_execution = job.next_run_time
    
    def _send_completion_alert(self, run_id: int, stats: Dict[str, Any]):
        """Send email alert for successful completion (placeholder)"""
        logger.info(f"📧 Completion alert would be sent for run {run_id}")
        # TODO: Implement email notifications using SMTP or email service
    
    def _send_failure_alert(self, error_message: str):
        """Send email alert for automation failure (placeholder)"""
        logger.error(f"📧 Failure alert would be sent: {error_message}")
        # TODO: Implement email notifications using SMTP or email service
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get current scheduler status and statistics"""
        status = {
            'scheduler_running': self.scheduler and self.scheduler.running,
            'next_runs': [],
            'recent_runs': [],
            'configuration': {}
        }
        
        if self.scheduler:
            # Get scheduled jobs
            for job in self.scheduler.get_jobs():
                status['next_runs'].append({
                    'job_id': job.id,
                    'job_name': job.name,
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None
                })
        
        # Get recent runs from database
        with self.db_manager.get_session() as session:
            recent_runs = session.query(AutomationRun).order_by(
                AutomationRun.started_at.desc()
            ).limit(10).all()
            
            for run in recent_runs:
                status['recent_runs'].append({
                    'id': run.id,
                    'run_type': run.run_type,
                    'status': run.status,
                    'started_at': run.started_at.isoformat() if run.started_at else None,
                    'completed_at': run.completed_at.isoformat() if run.completed_at else None,
                    'duration_seconds': run.duration_seconds,
                    'applications_processed': run.applications_processed,
                    'companies_matched': run.companies_matched,
                    'error_count': run.error_count
                })
        
        # Get current configuration
        for key in self.default_config.keys():
            status['configuration'][key] = self._get_config(key, self.default_config[key])
        
        return status
    
    def _handle_crash_recovery(self):
        """Handle crash recovery for interrupted automation runs"""
        try:
            with self.db_manager.get_session() as session:
                # Find any runs that were in progress during a crash
                interrupted_runs = session.query(AutomationRun).filter(
                    AutomationRun.status == 'running',
                    AutomationRun.started_at > datetime.now() - timedelta(hours=6)  # Only recent ones
                ).all()
                
                for run in interrupted_runs:
                    logger.warning(f"🔧 Found interrupted automation run {run.run_id} - marking as failed")
                    run.status = 'failed'
                    run.completed_at = datetime.now()
                    run.error_message = 'Interrupted by application restart'
                    
                session.commit()
                
                if interrupted_runs:
                    logger.info(f"🔧 Crash recovery completed - marked {len(interrupted_runs)} interrupted runs as failed")
                else:
                    logger.info("✅ No interrupted runs found during crash recovery")
                    
        except Exception as e:
            logger.error(f"❌ Crash recovery failed: {str(e)}")
    
    def _store_companies_house_results(self, enriched_companies: List[Dict], run_id: str) -> int:
        """Store enriched Companies House results in database"""
        companies_added = 0
        
        try:
            with self.db_manager.get_session() as session:
                from models import Company, Officer, Appointment
                
                for company_data in enriched_companies:
                    try:
                        company_number = company_data.get('company_number', '')
                        
                        # Check if company already exists
                        existing_company = session.query(Company).filter(
                            Company.company_number == company_number
                        ).first()
                        
                        if existing_company:
                            logger.info(f"Company {company_number} already exists, skipping")
                            continue
                        
                        # Create new company record with proper address field names
                        address_data = company_data.get('address', {})
                        company = Company(
                            company_number=company_number,
                            company_name=company_data.get('title', ''),
                            company_status=company_data.get('company_status', ''),
                            company_type=company_data.get('company_type', ''),
                            date_of_creation=company_data.get('date_of_creation'),
                            sic_codes=company_data.get('sic_codes', []),
                            address_line_1=address_data.get('address_line_1', ''),
                            address_line_2=address_data.get('address_line_2', ''),
                            locality=address_data.get('locality', ''),
                            postal_code=address_data.get('postal_code', ''),
                            country=address_data.get('country', ''),
                            created_at=datetime.now()
                        )
                        
                        session.add(company)
                        session.flush()  # Get the company ID
                        
                        # Add officer information - handle duplicates
                        officers = company_data.get('officers', [])
                        for officer_data in officers:
                            # Create unique officer ID
                            officer_id = f"auto_{company_number}_{officer_data.get('name', '').replace(' ', '_')}"
                            
                            # Check if officer already exists
                            existing_officer = session.query(Officer).filter(
                                Officer.ch_officer_id == officer_id
                            ).first()
                            
                            if existing_officer:
                                # Use existing officer
                                officer = existing_officer
                            else:
                                # Create new officer
                                officer = Officer(
                                    ch_officer_id=officer_id,
                                    name=officer_data.get('name', ''),
                                    nationality=officer_data.get('nationality', ''),
                                    date_of_birth_month=officer_data.get('date_of_birth', {}).get('month'),
                                    date_of_birth_year=officer_data.get('date_of_birth', {}).get('year'),
                                    created_at=datetime.now()
                                )
                                session.add(officer)
                                session.flush()  # Get officer ID
                            
                            # Create appointment (check for duplicates)
                            existing_appointment = session.query(Appointment).filter(
                                Appointment.company_id == company.id,
                                Appointment.officer_id == officer.id
                            ).first()
                            
                            if not existing_appointment:
                                appointment = Appointment(
                                    company_id=company.id,
                                    officer_id=officer.id,
                                    role=officer_data.get('officer_role', ''),
                                    appointed_date=officer_data.get('appointed_on'),
                                    resigned_date=officer_data.get('resigned_on'),
                                    is_active=officer_data.get('resigned_on') is None,
                                    created_at=datetime.now()
                                )
                                session.add(appointment)
                        
                        # Store charge information and determine tier in enrichment data
                        charges = company_data.get('charges', [])
                        has_charges = charges and len(charges) > 0
                        
                        # USER REQUESTED: Tier categorization based on charge information
                        # Records WITHOUT charge information → "Sic 41100 (Raw Data)" tier
                        # Records WITH charge information → "Lender (No Contact)" tier
                        data_tier = "Lender (No Contact)" if has_charges else "Sic 41100 (Raw Data)"
                        
                        from models import EnrichmentData
                        enrichment_data = {
                            "charges": charges,
                            "data_tier": data_tier,
                            "has_charges": has_charges,
                            "source": "weekly_companies_house_automation",
                            "companies_house_url": f"https://find-and-update.company-information.service.gov.uk/company/{company_number}",
                            "officers_cleaned": self._format_officers_data(company_data.get('officers', [])),
                            "registered_office": self._format_address_data(address_data)
                        }
                        
                        enrichment = EnrichmentData(
                            company_id=company.id,
                            provider='weekly_companies_house_import',
                            success=True,
                            enrichment_data=enrichment_data,
                            created_at=datetime.now()
                        )
                        session.add(enrichment)
                        
                        companies_added += 1
                        logger.info(f"✅ Added company {company_number} to database")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to store company {company_data.get('company_number', 'unknown')}: {str(e)}")
                        continue
                
                session.commit()
                logger.info(f"💾 Successfully stored {companies_added} companies in database")
                
        except Exception as e:
            logger.error(f"❌ Database storage failed: {str(e)}")
            
        return companies_added
    
    def _format_officers_data(self, officers_list: List[Dict]) -> str:
        """Format officer data for display"""
        if not officers_list:
            return "No officer data available"
        
        formatted_officers = []
        for officer in officers_list:
            name = officer.get('name', 'Unknown')
            role = officer.get('officer_role', 'Unknown Role')
            formatted_officers.append(f"{name} ({role})")
        
        return "; ".join(formatted_officers)
    
    def _format_address_data(self, address_data: Dict) -> str:
        """Format address data for display"""
        if not address_data:
            return "No address available"
        
        address_parts = [
            address_data.get('address_line_1', ''),
            address_data.get('address_line_2', ''),
            address_data.get('locality', ''),
            address_data.get('postal_code', ''),
            address_data.get('country', '')
        ]
        
        # Filter out empty parts and join
        return ", ".join(filter(None, address_parts))
    
    def _on_job_executed(self, event):
        """APScheduler event listener for job execution completion"""
        try:
            job_id = event.job_id
            if event.exception:
                # Job failed
                logger.error(f"🚫 Job {job_id} failed with exception: {event.exception}")
                if hasattr(self, '_current_run_id') and self._current_run_id:
                    self.monitor.log_automation_error(
                        self._current_run_id,
                        f"job_execution:{job_id}",
                        event.exception
                    )
            else:
                # Job completed successfully
                logger.info(f"✅ Job {job_id} completed successfully")
        except Exception as e:
            logger.error(f"Error in job execution listener: {e}")
    
    def _on_job_submitted(self, event):
        """APScheduler event listener for job submission"""
        try:
            job_id = event.job_id
            logger.info(f"📤 Job {job_id} submitted for execution")
        except Exception as e:
            logger.error(f"Error in job submission listener: {e}")

# Global scheduler instance
scheduler_instance = None

def initialize_scheduler():
    """Initialize global scheduler instance with single-instance guarding"""
    global scheduler_instance
    if not scheduler_instance:
        try:
            scheduler_instance = WeeklyAutomationScheduler()
            
            # Initialize scheduler with persistence
            if scheduler_instance.initialize_scheduler():
                # Start the scheduler
                if scheduler_instance.start_scheduler():
                    logger.info("🚀 Global scheduler instance created and started successfully")
                else:
                    logger.error("❌ Failed to start global scheduler instance")
                    scheduler_instance = None
            else:
                logger.error("❌ Failed to initialize global scheduler instance")
                scheduler_instance = None
                
        except Exception as e:
            logger.error(f"❌ Exception creating global scheduler instance: {str(e)}")
            scheduler_instance = None
            
    return scheduler_instance

def get_scheduler():
    """Get global scheduler instance"""
    global scheduler_instance
    return scheduler_instance

if __name__ == "__main__":
    # Run as standalone script for testing
    logging.basicConfig(level=logging.INFO)
    
    scheduler = WeeklyAutomationScheduler()
    
    if scheduler.start_scheduler():
        logger.info("🚀 Scheduler started - running in background")
        
        try:
            # Keep alive
            while True:
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        finally:
            scheduler.stop_scheduler()
    else:
        logger.error("❌ Failed to start scheduler")