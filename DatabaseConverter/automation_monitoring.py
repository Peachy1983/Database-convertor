"""
Comprehensive monitoring and alerting system for the weekly automation pipeline.
Provides detailed logging, performance tracking, error handling, and email notifications.
"""
import os
import logging
import smtplib
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from email.mime.text import MIMEText as MimeText
from email.mime.multipart import MIMEMultipart as MimeMultipart
from dataclasses import dataclass

from database import DatabaseManager
from models import AutomationRun

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class AlertConfig:
    """Configuration for email alerts"""
    enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    username: str = ""
    password: str = ""
    from_email: str = ""
    to_emails: List[str] = None
    use_tls: bool = True

class AutomationMonitor:
    """
    Comprehensive monitoring system for automation pipeline.
    Tracks performance, handles errors, and sends alerts.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.alert_config = self._load_alert_config()
        
        # Performance thresholds
        self.thresholds = {
            'max_duration_minutes': 120,  # 2 hours max
            'min_success_rate': 80,  # 80% minimum success rate
            'max_error_rate': 20,  # 20% maximum error rate
            'min_applications_processed': 1,  # At least 1 application processed
        }
        
        logger.info("Automation monitor initialized")
    
    def _load_alert_config(self) -> AlertConfig:
        """Load email alert configuration from environment variables"""
        return AlertConfig(
            enabled=os.getenv("AUTOMATION_ALERTS_ENABLED", "false").lower() == "true",
            smtp_server=os.getenv("SMTP_SERVER", ""),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            username=os.getenv("SMTP_USERNAME", ""),
            password=os.getenv("SMTP_PASSWORD", ""),
            from_email=os.getenv("AUTOMATION_FROM_EMAIL", ""),
            to_emails=os.getenv("AUTOMATION_TO_EMAILS", "").split(",") if os.getenv("AUTOMATION_TO_EMAILS") else [],
            use_tls=os.getenv("SMTP_USE_TLS", "true").lower() == "true"
        )
    
    def log_automation_start(self, run_type: str, config: Dict[str, Any]) -> int:
        """Log the start of an automation run"""
        try:
            run_id = self.db_manager.create_automation_run(run_type, config)
            
            logger.info(f"🚀 Automation run {run_id} started")
            logger.info(f"   Run type: {run_type}")
            logger.info(f"   Configuration: {json.dumps(config, indent=2)}")
            
            return run_id
            
        except Exception as e:
            logger.error(f"❌ Failed to log automation start: {str(e)}")
            raise
    
    def log_automation_progress(self, run_id: int, stage: str, progress: Dict[str, Any]):
        """Log progress during automation run"""
        try:
            logger.info(f"📊 Run {run_id} - {stage}: {json.dumps(progress, default=str)}")
            
            # Update run statistics
            self.db_manager.update_automation_run(run_id, progress)
            
        except Exception as e:
            logger.error(f"❌ Failed to log progress for run {run_id}: {str(e)}")
    
    def log_automation_error(self, run_id: int, stage: str, error: Exception, context: Dict[str, Any] = None):
        """Log errors during automation run"""
        try:
            error_msg = f"Error in {stage}: {str(error)}"
            
            logger.error(f"❌ Run {run_id} - {error_msg}")
            if context:
                logger.error(f"   Context: {json.dumps(context, default=str)}")
            
            # Update error count
            current_run = self.db_manager.get_automation_runs(limit=1)
            if current_run:
                current_errors = current_run[0].get('error_count', 0)
                current_details = current_run[0].get('error_details', '')
                
                new_details = f"{current_details}; {error_msg}" if current_details else error_msg
                
                self.db_manager.update_automation_run(run_id, {
                    'error_count': current_errors + 1,
                    'error_details': new_details[:2000]  # Limit length
                })
            
        except Exception as e:
            logger.error(f"❌ Failed to log error for run {run_id}: {str(e)}")
    
    def complete_automation_run(self, run_id: int, final_stats: Dict[str, Any]):
        """Complete automation run and perform final monitoring checks"""
        try:
            # Determine final status
            error_count = final_stats.get('error_count', 0)
            applications_processed = final_stats.get('applications_processed', 0)
            
            if error_count == 0 and applications_processed > 0:
                status = 'completed'
            elif error_count > 0 and applications_processed > 0:
                status = 'partial'
            elif applications_processed == 0:
                status = 'failed'
            else:
                status = 'failed'
            
            # Complete the run
            self.db_manager.complete_automation_run(run_id, status)
            self.db_manager.update_automation_run(run_id, final_stats)
            
            logger.info(f"✅ Automation run {run_id} completed with status: {status}")
            logger.info(f"   Final statistics: {json.dumps(final_stats, default=str)}")
            
            # Check performance and send alerts if needed
            self._check_performance_thresholds(run_id, final_stats)
            
            # Send completion alert if configured
            if self.alert_config.enabled:
                if status == 'failed':
                    self._send_failure_alert(run_id, final_stats)
                elif status == 'completed':
                    self._send_success_alert(run_id, final_stats)
                elif status == 'partial':
                    self._send_warning_alert(run_id, final_stats)
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Failed to complete automation run {run_id}: {str(e)}")
            return 'failed'
    
    def _check_performance_thresholds(self, run_id: int, stats: Dict[str, Any]):
        """Check if automation run meets performance thresholds"""
        try:
            issues = []
            
            # Check duration
            duration_minutes = stats.get('duration_seconds', 0) / 60
            if duration_minutes > self.thresholds['max_duration_minutes']:
                issues.append(f"Duration exceeded threshold: {duration_minutes:.1f}m > {self.thresholds['max_duration_minutes']}m")
            
            # Check error rate
            total_operations = stats.get('applications_processed', 0)
            error_count = stats.get('error_count', 0)
            if total_operations > 0:
                error_rate = (error_count / total_operations) * 100
                if error_rate > self.thresholds['max_error_rate']:
                    issues.append(f"Error rate exceeded threshold: {error_rate:.1f}% > {self.thresholds['max_error_rate']}%")
            
            # Check minimum processing
            applications_processed = stats.get('applications_processed', 0)
            if applications_processed < self.thresholds['min_applications_processed']:
                issues.append(f"Too few applications processed: {applications_processed} < {self.thresholds['min_applications_processed']}")
            
            # Log issues
            if issues:
                logger.warning(f"⚠️ Run {run_id} performance issues:")
                for issue in issues:
                    logger.warning(f"   - {issue}")
                
                # Send performance alert
                if self.alert_config.enabled:
                    self._send_performance_alert(run_id, issues, stats)
            else:
                logger.info(f"✅ Run {run_id} met all performance thresholds")
                
        except Exception as e:
            logger.error(f"❌ Failed to check performance thresholds: {str(e)}")
    
    def get_performance_summary(self, days_back: int = 30) -> Dict[str, Any]:
        """Get comprehensive performance summary for monitoring dashboard"""
        try:
            # Get automation statistics
            stats = self.db_manager.get_automation_statistics()
            
            # Calculate additional metrics
            recent_runs = stats.get('recent_runs', [])
            
            # Performance over time
            daily_performance = self._calculate_daily_performance(recent_runs, days_back)
            
            # Error analysis
            error_analysis = self._analyze_errors(recent_runs)
            
            # Trend analysis
            trends = self._calculate_trends(recent_runs)
            
            summary = {
                'basic_stats': stats,
                'daily_performance': daily_performance,
                'error_analysis': error_analysis,
                'trends': trends,
                'last_updated': datetime.now().isoformat()
            }
            
            logger.info(f"📊 Generated performance summary for last {days_back} days")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Failed to generate performance summary: {str(e)}")
            return {}
    
    def _calculate_daily_performance(self, runs: List[Dict], days_back: int) -> List[Dict]:
        """Calculate daily performance metrics"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            daily_data = {}
            
            for run in runs:
                if not run.get('started_at'):
                    continue
                
                run_date = datetime.fromisoformat(run['started_at'].replace('Z', '+00:00'))
                if run_date < cutoff_date:
                    continue
                
                date_key = run_date.date().isoformat()
                
                if date_key not in daily_data:
                    daily_data[date_key] = {
                        'date': date_key,
                        'runs': 0,
                        'successful_runs': 0,
                        'failed_runs': 0,
                        'applications_processed': 0,
                        'companies_matched': 0,
                        'errors': 0,
                        'avg_duration': 0,
                        'duration_sum': 0
                    }
                
                day_data = daily_data[date_key]
                day_data['runs'] += 1
                day_data['applications_processed'] += run.get('applications_processed', 0)
                day_data['companies_matched'] += run.get('companies_matched', 0)
                day_data['errors'] += run.get('error_count', 0)
                
                if run.get('duration_seconds'):
                    day_data['duration_sum'] += run['duration_seconds']
                
                if run.get('status') == 'completed':
                    day_data['successful_runs'] += 1
                elif run.get('status') == 'failed':
                    day_data['failed_runs'] += 1
            
            # Calculate averages
            for day_data in daily_data.values():
                if day_data['runs'] > 0:
                    day_data['avg_duration'] = day_data['duration_sum'] / day_data['runs'] / 60  # Convert to minutes
                    day_data['success_rate'] = (day_data['successful_runs'] / day_data['runs']) * 100
                
                del day_data['duration_sum']  # Remove temporary field
            
            return sorted(daily_data.values(), key=lambda x: x['date'])
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate daily performance: {str(e)}")
            return []
    
    def _analyze_errors(self, runs: List[Dict]) -> Dict[str, Any]:
        """Analyze error patterns and frequencies"""
        try:
            error_analysis = {
                'total_errors': 0,
                'error_categories': {},
                'most_common_errors': [],
                'error_trend': 'stable'
            }
            
            error_messages = []
            recent_error_counts = []
            
            for run in runs[-10:]:  # Last 10 runs
                error_count = run.get('error_count', 0)
                error_analysis['total_errors'] += error_count
                recent_error_counts.append(error_count)
                
                if run.get('error_details'):
                    error_messages.extend(run['error_details'].split(';'))
            
            # Categorize errors
            for error_msg in error_messages:
                error_msg = error_msg.strip().lower()
                if 'api' in error_msg or 'request' in error_msg:
                    category = 'API Errors'
                elif 'database' in error_msg or 'sql' in error_msg:
                    category = 'Database Errors'
                elif 'timeout' in error_msg:
                    category = 'Timeout Errors'
                elif 'rate limit' in error_msg:
                    category = 'Rate Limit Errors'
                else:
                    category = 'Other Errors'
                
                error_analysis['error_categories'][category] = error_analysis['error_categories'].get(category, 0) + 1
            
            # Determine trend
            if len(recent_error_counts) >= 5:
                recent_avg = sum(recent_error_counts[-5:]) / 5
                older_avg = sum(recent_error_counts[-10:-5]) / 5 if len(recent_error_counts) >= 10 else recent_avg
                
                if recent_avg > older_avg * 1.2:
                    error_analysis['error_trend'] = 'increasing'
                elif recent_avg < older_avg * 0.8:
                    error_analysis['error_trend'] = 'decreasing'
                else:
                    error_analysis['error_trend'] = 'stable'
            
            # Most common error categories
            error_analysis['most_common_errors'] = sorted(
                error_analysis['error_categories'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            return error_analysis
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze errors: {str(e)}")
            return {}
    
    def _calculate_trends(self, runs: List[Dict]) -> Dict[str, str]:
        """Calculate performance trends"""
        try:
            if len(runs) < 4:
                return {'applications': 'insufficient_data', 'companies': 'insufficient_data', 'duration': 'insufficient_data'}
            
            # Get recent and older runs
            recent_runs = runs[-5:] if len(runs) >= 5 else runs
            older_runs = runs[-10:-5] if len(runs) >= 10 else []
            
            if not older_runs:
                return {'applications': 'insufficient_data', 'companies': 'insufficient_data', 'duration': 'insufficient_data'}
            
            # Calculate averages
            recent_apps = sum(run.get('applications_processed', 0) for run in recent_runs) / len(recent_runs)
            older_apps = sum(run.get('applications_processed', 0) for run in older_runs) / len(older_runs)
            
            recent_companies = sum(run.get('companies_matched', 0) for run in recent_runs) / len(recent_runs)
            older_companies = sum(run.get('companies_matched', 0) for run in older_runs) / len(older_runs)
            
            recent_duration = sum(run.get('duration_seconds', 0) for run in recent_runs if run.get('duration_seconds')) / len([r for r in recent_runs if r.get('duration_seconds')])
            older_duration = sum(run.get('duration_seconds', 0) for run in older_runs if run.get('duration_seconds')) / len([r for r in older_runs if r.get('duration_seconds')])
            
            def trend_direction(recent: float, older: float) -> str:
                if older == 0:
                    return 'stable'
                change = (recent - older) / older
                if change > 0.1:
                    return 'increasing'
                elif change < -0.1:
                    return 'decreasing'
                else:
                    return 'stable'
            
            return {
                'applications': trend_direction(recent_apps, older_apps),
                'companies': trend_direction(recent_companies, older_companies),
                'duration': trend_direction(recent_duration, older_duration)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate trends: {str(e)}")
            return {}
    
    def _send_success_alert(self, run_id: int, stats: Dict[str, Any]):
        """Send success notification"""
        try:
            subject = f"✅ Automation Run {run_id} Completed Successfully"
            
            body = f"""
Automation run {run_id} has completed successfully!

📊 Summary:
• Applications Processed: {stats.get('applications_processed', 0)}
• Companies Matched: {stats.get('companies_matched', 0)}
• Contacts Enriched: {stats.get('contacts_enriched', 0)}
• Duration: {stats.get('duration_seconds', 0) / 60:.1f} minutes
• Errors: {stats.get('error_count', 0)}

🎉 The automation pipeline processed new planning applications and enriched the database with fresh business intelligence data.
            """
            
            self._send_email_alert(subject, body)
            logger.info(f"📧 Success alert sent for run {run_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send success alert: {str(e)}")
    
    def _send_failure_alert(self, run_id: int, stats: Dict[str, Any]):
        """Send failure notification"""
        try:
            subject = f"❌ Automation Run {run_id} Failed"
            
            body = f"""
⚠️ AUTOMATION FAILURE ALERT ⚠️

Automation run {run_id} has failed and requires attention.

📊 Details:
• Duration: {stats.get('duration_seconds', 0) / 60:.1f} minutes
• Applications Processed: {stats.get('applications_processed', 0)}
• Error Count: {stats.get('error_count', 0)}
• Error Details: {stats.get('error_details', 'No specific details available')}

🔧 Action Required:
Please check the automation dashboard and logs to identify and resolve the issue.
            """
            
            self._send_email_alert(subject, body)
            logger.info(f"📧 Failure alert sent for run {run_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send failure alert: {str(e)}")
    
    def _send_warning_alert(self, run_id: int, stats: Dict[str, Any]):
        """Send warning notification for partial success"""
        try:
            subject = f"⚠️ Automation Run {run_id} Completed with Warnings"
            
            body = f"""
Automation run {run_id} completed but encountered some issues.

📊 Summary:
• Applications Processed: {stats.get('applications_processed', 0)}
• Companies Matched: {stats.get('companies_matched', 0)}
• Contacts Enriched: {stats.get('contacts_enriched', 0)}
• Duration: {stats.get('duration_seconds', 0) / 60:.1f} minutes
• Errors: {stats.get('error_count', 0)}

⚠️ Issues Encountered:
{stats.get('error_details', 'No specific details available')}

The automation pipeline processed some data successfully but encountered errors that may require attention.
            """
            
            self._send_email_alert(subject, body)
            logger.info(f"📧 Warning alert sent for run {run_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send warning alert: {str(e)}")
    
    def _send_performance_alert(self, run_id: int, issues: List[str], stats: Dict[str, Any]):
        """Send performance threshold alert"""
        try:
            subject = f"📊 Performance Alert - Run {run_id}"
            
            body = f"""
Performance thresholds exceeded for automation run {run_id}.

⚠️ Issues Detected:
{chr(10).join(f'• {issue}' for issue in issues)}

📊 Run Statistics:
• Duration: {stats.get('duration_seconds', 0) / 60:.1f} minutes
• Applications Processed: {stats.get('applications_processed', 0)}
• Error Count: {stats.get('error_count', 0)}

Please review the automation configuration and system performance.
            """
            
            self._send_email_alert(subject, body)
            logger.info(f"📧 Performance alert sent for run {run_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send performance alert: {str(e)}")
    
    def _send_email_alert(self, subject: str, body: str):
        """Send email alert using SMTP"""
        if not self.alert_config.enabled or not self.alert_config.to_emails:
            return
        
        try:
            # Create message
            msg = MimeMultipart()
            msg['From'] = self.alert_config.from_email
            msg['To'] = ", ".join(self.alert_config.to_emails)
            msg['Subject'] = subject
            
            msg.attach(MimeText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.alert_config.smtp_server, self.alert_config.smtp_port)
            
            if self.alert_config.use_tls:
                server.starttls()
            
            if self.alert_config.username and self.alert_config.password:
                server.login(self.alert_config.username, self.alert_config.password)
            
            text = msg.as_string()
            server.sendmail(self.alert_config.from_email, self.alert_config.to_emails, text)
            server.quit()
            
            logger.info(f"📧 Email alert sent to {len(self.alert_config.to_emails)} recipients")
            
        except Exception as e:
            logger.error(f"❌ Failed to send email alert: {str(e)}")

def get_monitor(db_manager: DatabaseManager) -> AutomationMonitor:
    """Get automation monitor instance"""
    return AutomationMonitor(db_manager)