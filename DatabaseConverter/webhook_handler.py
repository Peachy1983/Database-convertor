from flask import Flask, request, jsonify
import json
import os
from datetime import datetime
from typing import Dict, Any, List
import logging

from database import DatabaseManager
from applicant_processor import ApplicantProcessor
from applicant_pipeline import ApplicantPipeline
from models import PlanningApplication, Applicant, LinkedHelperConnection

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize processors and database
db_manager = DatabaseManager()
applicant_processor = ApplicantProcessor()

# Initialize complete pipeline
companies_house_key = os.getenv('COMPANIES_HOUSE_API_KEY', '')
pipeline = ApplicantPipeline(db_manager, companies_house_key) if companies_house_key else None

class LinkedHelperWebhookHandler:
    """Handle LinkedIn Helper webhook notifications"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def process_webhook_data(self, webhook_data: Dict[str, Any]) -> bool:
        """Process incoming webhook data from LinkedHelper"""
        try:
            # Extract data from webhook payload
            event_type = webhook_data.get('event_type', '')
            contact_data = webhook_data.get('contact', {})
            
            # Extract contact information
            full_name = contact_data.get('full_name', contact_data.get('name', ''))
            first_name = contact_data.get('first_name', '')
            last_name = contact_data.get('last_name', '')
            company = contact_data.get('company', '')
            position = contact_data.get('position', '')
            linkedin_url = contact_data.get('linkedin_url', contact_data.get('profile_url', ''))
            
            # Map event types to connection status
            status_mapping = {
                'connection_sent': 'Pending',
                'connection_accepted': 'Connected',
                'connection_declined': 'Declined',
                'message_sent': 'Pending',
                'message_replied': 'Connected',
                'profile_visited': 'Visited'
            }
            
            connection_status = status_mapping.get(event_type, event_type)
            
            # Additional data
            date_connected = webhook_data.get('timestamp', datetime.now().isoformat())
            message_sent = contact_data.get('last_message', '')
            replied = 'Yes' if event_type == 'message_replied' else 'No'
            
            # Save to database using SQLAlchemy
            connection_data = {
                'full_name': full_name,
                'first_name': first_name,
                'last_name': last_name,
                'company': company,
                'position': position,
                'linkedin_url': linkedin_url,
                'connection_status': connection_status,
                'date_connected': date_connected,
                'message_sent': message_sent,
                'replied': replied
            }
            
            # Use DatabaseManager to save LinkedIn connection
            connection_id = self.db_manager.save_linkedin_connection(connection_data)
            
            if connection_id:
                logging.info(f"Successfully saved LinkedIn connection: {full_name}")
                return True
            else:
                logging.error(f"Failed to save LinkedIn connection: {full_name}")
                return False
                
        except Exception as e:
            logging.error(f"Error processing webhook data: {str(e)}")
            return False

webhook_handler = LinkedHelperWebhookHandler(db_manager)

@app.route('/webhook/linkedhelper', methods=['POST'])
def handle_linkedhelper_webhook():
    """Handle incoming LinkedHelper webhook"""
    try:
        # Get JSON data from webhook
        webhook_data = request.get_json()
        
        if not webhook_data:
            return jsonify({'error': 'No data received'}), 400
        
        # Log the webhook for debugging
        logging.info(f"Received LinkedHelper webhook: {json.dumps(webhook_data, indent=2)}")
        
        # Process the webhook data
        success = webhook_handler.process_webhook_data(webhook_data)
        
        if success:
            return jsonify({'status': 'success', 'message': 'Webhook processed successfully'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to process webhook'}), 500
            
    except Exception as e:
        logging.error(f"Webhook handler error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/webhook/linkedhelper/test', methods=['GET'])
def test_webhook():
    """Test endpoint to verify webhook is working"""
    return jsonify({
        'status': 'ok',
        'message': 'LinkedHelper webhook endpoint is active',
        'timestamp': datetime.now().isoformat()
    })

class ApplicantDataHandler:
    """Handle planning applicant data extraction and processing"""
    
    def __init__(self, db_manager: DatabaseManager, processor: ApplicantProcessor):
        self.db_manager = db_manager
        self.processor = processor
        
    def process_applicant_batch(self, batch_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of planning applicant data"""
        processed_count = 0
        error_count = 0
        errors = []
        
        try:
            with self.db_manager.get_session() as session:
                for applicant_data in batch_data:
                    try:
                        # Validate applicant data
                        is_valid, validation_msg = self.processor.validate_applicant_data(applicant_data)
                        if not is_valid:
                            error_count += 1
                            errors.append(f"Invalid data: {validation_msg}")
                            continue
                        
                        # Normalize applicant data
                        normalized_data = self.processor.normalize_applicant_data(applicant_data)
                        
                        # Check for existing planning application
                        planning_ref = normalized_data['planning_reference']
                        borough = normalized_data.get('borough', '')
                        
                        planning_app = session.query(PlanningApplication).filter(
                            PlanningApplication.reference == planning_ref,
                            PlanningApplication.borough == borough
                        ).first()
                        
                        if not planning_app:
                            # Create new planning application record
                            planning_app = PlanningApplication(
                                borough=borough,
                                reference=planning_ref,
                                description=applicant_data.get('description', ''),
                                raw_data=applicant_data
                            )
                            session.add(planning_app)
                            session.flush()  # Get the ID
                        
                        # Check for existing applicant to avoid duplicates
                        existing_applicant = session.query(Applicant).filter(
                            Applicant.planning_application_id == planning_app.id,
                            Applicant.normalized_name == normalized_data['normalized_name']
                        ).first()
                        
                        if not existing_applicant:
                            # Create new applicant record
                            applicant = Applicant(
                                planning_application_id=planning_app.id,
                                raw_name=normalized_data['raw_name'],
                                normalized_name=normalized_data['normalized_name'],
                                applicant_type=normalized_data['applicant_type'],
                                contact_email=normalized_data.get('contact_email'),
                                contact_phone=normalized_data.get('contact_phone'),
                                contact_address=normalized_data.get('contact_address')
                            )
                            session.add(applicant)
                            processed_count += 1
                        else:
                            logging.info(f"Duplicate applicant skipped: {normalized_data['raw_name']} for {planning_ref}")
                        
                    except Exception as e:
                        error_count += 1
                        errors.append(f"Processing error: {str(e)}")
                        logging.error(f"Error processing applicant: {str(e)}")
                        continue
                
                session.commit()
                
        except Exception as e:
            error_count += len(batch_data)
            errors.append(f"Database error: {str(e)}")
            logging.error(f"Database error in batch processing: {str(e)}")
        
        return {
            'processed': processed_count,
            'errors': error_count,
            'error_messages': errors
        }

applicant_handler = ApplicantDataHandler(db_manager, applicant_processor)

@app.route('/api/applicants/pipeline', methods=['POST'])
def handle_applicant_pipeline():
    """Handle batch processing using the complete pipeline"""
    if not pipeline:
        return jsonify({
            'error': 'Pipeline not configured',
            'message': 'Companies House API key not configured'
        }), 503
    
    try:
        # Check for API key in headers for security
        api_key = request.headers.get('X-API-Key')
        expected_key = os.getenv('PLANNING_API_KEY', 'default-key-123')
        
        if not api_key or api_key != expected_key:
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Valid API key required'
            }), 401
        
        # Get JSON data from request
        request_data = request.get_json()
        
        if not request_data:
            return jsonify({
                'error': 'No data received',
                'message': 'Request body must contain JSON data'
            }), 400
        
        # Extract applicants list
        applicants = request_data.get('applicants', [])
        if not isinstance(applicants, list):
            return jsonify({
                'error': 'Invalid data format',
                'message': 'Request must contain an "applicants" array'
            }), 400
        
        if len(applicants) == 0:
            return jsonify({
                'error': 'Empty batch',
                'message': 'No applicants provided in batch'
            }), 400
        
        # Log the pipeline processing
        logging.info(f"Processing {len(applicants)} applicants through complete pipeline")
        
        # Process through complete pipeline
        pipeline_stats = pipeline.process_applicant_batch(applicants)
        
        # Prepare response
        response = {
            'status': 'success' if len(pipeline_stats['errors']) == 0 else 'partial_success',
            'message': f"Pipeline processed {pipeline_stats['processed_applicants']} applicants",
            'pipeline_stats': pipeline_stats,
            'total_received': len(applicants)
        }
        
        # Include error details if any
        if pipeline_stats['errors']:
            response['errors'] = pipeline_stats['errors'][:10]  # Limit to first 10 errors
        
        status_code = 200 if len(pipeline_stats['errors']) == 0 else 207  # 207 Multi-Status for partial success
        
        return jsonify(response), status_code
        
    except Exception as e:
        logging.error(f"Pipeline handler error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Server error: {str(e)}'
        }), 500

@app.route('/api/pipeline/status', methods=['GET'])
def get_pipeline_status():
    """Get pipeline status and configuration"""
    if not pipeline:
        return jsonify({
            'configured': False,
            'message': 'Companies House API key not configured'
        }), 503
    
    try:
        status = pipeline.get_pipeline_status()
        connectivity = pipeline.test_pipeline_connectivity()
        
        return jsonify({
            'configured': True,
            'status': status,
            'connectivity': connectivity,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logging.error(f"Pipeline status error: {str(e)}")
        return jsonify({
            'error': 'Status check failed',
            'message': str(e)
        }), 500

@app.route('/api/applicants/batch', methods=['POST'])
def handle_applicant_batch():
    """Handle batch processing of planning applicant data"""
    try:
        # Check for API key in headers for security
        api_key = request.headers.get('X-API-Key')
        expected_key = os.getenv('PLANNING_API_KEY', 'default-key-123')
        
        if not api_key or api_key != expected_key:
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Valid API key required'
            }), 401
        
        # Get JSON data from request
        request_data = request.get_json()
        
        if not request_data:
            return jsonify({
                'error': 'No data received',
                'message': 'Request body must contain JSON data'
            }), 400
        
        # Extract applicants list
        applicants = request_data.get('applicants', [])
        if not isinstance(applicants, list):
            return jsonify({
                'error': 'Invalid data format',
                'message': 'Request must contain an "applicants" array'
            }), 400
        
        if len(applicants) == 0:
            return jsonify({
                'error': 'Empty batch',
                'message': 'No applicants provided in batch'
            }), 400
        
        # Log the batch processing
        logging.info(f"Processing batch of {len(applicants)} applicants")
        
        # Process the batch
        result = applicant_handler.process_applicant_batch(applicants)
        
        # Prepare response
        response = {
            'status': 'success' if result['errors'] == 0 else 'partial_success',
            'message': f"Processed {result['processed']} applicants, {result['errors']} errors",
            'processed_count': result['processed'],
            'error_count': result['errors'],
            'total_received': len(applicants)
        }
        
        # Include error details if any
        if result['error_messages']:
            response['error_details'] = result['error_messages'][:10]  # Limit to first 10 errors
        
        status_code = 200 if result['errors'] == 0 else 207  # 207 Multi-Status for partial success
        
        return jsonify(response), status_code
        
    except Exception as e:
        logging.error(f"Applicant batch handler error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Server error: {str(e)}'
        }), 500

@app.route('/api/applicants/test', methods=['GET'])
def test_applicant_endpoint():
    """Test endpoint to verify applicant processing is working"""
    return jsonify({
        'status': 'ok',
        'message': 'Applicant batch processing endpoint is active',
        'timestamp': datetime.now().isoformat(),
        'expected_format': {
            'applicants': [
                {
                    'planning_reference': 'required - string',
                    'applicant_name': 'required - string',
                    'borough': 'optional - string',
                    'contact_email': 'optional - string',
                    'contact_phone': 'optional - string',
                    'contact_address': 'optional - string',
                    'description': 'optional - string'
                }
            ]
        }
    })

if __name__ == '__main__':
    # Production-safe settings - disable debug mode and reloader for security
    app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)