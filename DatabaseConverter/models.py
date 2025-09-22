"""
SQLAlchemy models for the developer-lender intelligence system.
Comprehensive PostgreSQL schema with proper relationships and indexes.
"""
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, Float, ForeignKey, ARRAY, JSON, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

Base = declarative_base()

class PlanningApplication(Base):
    """Planning applications from UK councils"""
    __tablename__ = 'planning_applications'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    borough = Column(String(100), nullable=False)
    reference = Column(String(50), nullable=False)
    application_type = Column(String(100))
    status = Column(String(50))
    decision_date = Column(DateTime)
    received_date = Column(DateTime)
    start_date = Column(DateTime)
    description = Column(Text)
    is_outline = Column(Boolean, default=False)
    latitude = Column(Float)
    longitude = Column(Float)
    planning_url = Column(Text)
    organisation = Column(String(200))
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Unique constraint on borough + reference
    __table_args__ = (
        Index('idx_planning_borough_ref', 'borough', 'reference', unique=True),
        Index('idx_planning_status', 'status'),
        Index('idx_planning_type', 'application_type'),
        Index('idx_planning_dates', 'decision_date', 'received_date'),
        Index('idx_planning_location', 'latitude', 'longitude'),
    )
    
    # Relationships
    applicants = relationship("Applicant", back_populates="planning_application", cascade="all, delete-orphan")

class Applicant(Base):
    """Applicants from planning applications"""
    __tablename__ = 'applicants'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    planning_application_id = Column(Integer, ForeignKey('planning_applications.id', ondelete='CASCADE'), nullable=False)
    raw_name = Column(String(500), nullable=False)
    normalized_name = Column(String(500))
    applicant_type = Column(String(50))  # individual, company, organization
    contact_email = Column(String(200))
    contact_phone = Column(String(50))
    contact_address = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_applicant_planning', 'planning_application_id'),
        Index('idx_applicant_name', 'normalized_name'),
        Index('idx_applicant_type', 'applicant_type'),
    )
    
    # Relationships
    planning_application = relationship("PlanningApplication", back_populates="applicants")
    company_matches = relationship("ApplicantCompanyMatch", back_populates="applicant", cascade="all, delete-orphan")

class Company(Base):
    """Enhanced Companies table with better structure"""
    __tablename__ = 'companies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_number = Column(String(20), unique=True, nullable=False)
    company_name = Column(String(500))
    company_status = Column(String(50))
    company_type = Column(String(100))
    jurisdiction = Column(String(50))
    date_of_creation = Column(DateTime)
    date_of_cessation = Column(DateTime)
    
    # Address fields (normalized)
    address_line_1 = Column(String(200))
    address_line_2 = Column(String(200))
    locality = Column(String(100))
    region = Column(String(100))
    postal_code = Column(String(20))
    country = Column(String(50))
    
    # SIC codes as PostgreSQL array
    sic_codes = Column(ARRAY(String))
    
    # Store raw JSON data from Companies House
    raw_json = Column(JSON)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_company_number', 'company_number', unique=True),
        Index('idx_company_name', 'company_name'),
        Index('idx_company_status', 'company_status'),
        Index('idx_company_type', 'company_type'),
        Index('idx_company_location', 'postal_code', 'country'),
        Index('idx_company_creation', 'date_of_creation'),
    )
    
    # Relationships
    appointments = relationship("Appointment", back_populates="company", cascade="all, delete-orphan")
    enrichment_data = relationship("EnrichmentData", back_populates="company", cascade="all, delete-orphan")
    processing_logs = relationship("ProcessingLog", back_populates="company", cascade="all, delete-orphan")
    company_matches = relationship("ApplicantCompanyMatch", back_populates="company", cascade="all, delete-orphan")
    contacts = relationship("Contact", foreign_keys="[Contact.company_id]", back_populates="company", cascade="all, delete-orphan")
    planning_data = relationship("PlanningData", back_populates="company", cascade="all, delete-orphan")

class Officer(Base):
    """Company officers from Companies House"""
    __tablename__ = 'officers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ch_officer_id = Column(String(100), unique=True, nullable=False)  # Companies House officer ID
    name = Column(String(200), nullable=False)
    
    # Date of birth fields (as per Companies House format)
    date_of_birth_month = Column(Integer)
    date_of_birth_year = Column(Integer)
    
    nationality = Column(String(50))
    occupation = Column(String(200))
    
    # Address fields
    address_line_1 = Column(String(200))
    address_line_2 = Column(String(200))
    locality = Column(String(100))
    region = Column(String(100))
    postal_code = Column(String(20))
    country = Column(String(50))
    
    # Store raw JSON data from Companies House
    raw_json = Column(JSON)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_officer_ch_id', 'ch_officer_id', unique=True),
        Index('idx_officer_name', 'name'),
        Index('idx_officer_nationality', 'nationality'),
        Index('idx_officer_dob', 'date_of_birth_year', 'date_of_birth_month'),
    )
    
    # Relationships
    appointments = relationship("Appointment", back_populates="officer", cascade="all, delete-orphan")
    contacts = relationship("Contact", foreign_keys="[Contact.officer_id]", back_populates="officer", cascade="all, delete-orphan")

class Appointment(Base):
    """Officer appointments at companies"""
    __tablename__ = 'appointments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    officer_id = Column(Integer, ForeignKey('officers.id', ondelete='CASCADE'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    
    role = Column(String(100), nullable=False)  # director, secretary, etc.
    appointed_date = Column(DateTime)
    resigned_date = Column(DateTime)
    
    # Generated column for active status
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Store raw appointment data
    raw_json = Column(JSON)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_appointment_officer', 'officer_id'),
        Index('idx_appointment_company', 'company_id'),
        Index('idx_appointment_role', 'role'),
        Index('idx_appointment_active', 'is_active'),
        Index('idx_appointment_dates', 'appointed_date', 'resigned_date'),
        Index('idx_appointment_unique', 'officer_id', 'company_id', 'role', 'appointed_date', unique=True),
    )
    
    # Relationships
    officer = relationship("Officer", back_populates="appointments")
    company = relationship("Company", back_populates="appointments")

class ApplicantCompanyMatch(Base):
    """Matches between planning applicants and Companies House companies"""
    __tablename__ = 'applicant_company_matches'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    applicant_id = Column(Integer, ForeignKey('applicants.id', ondelete='CASCADE'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    
    match_method = Column(String(50), nullable=False)  # exact_name, fuzzy_name, address, etc.
    confidence_score = Column(Float)  # 0.0 to 1.0
    verified = Column(Boolean, default=False)  # manually verified
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_match_applicant', 'applicant_id'),
        Index('idx_match_company', 'company_id'),
        Index('idx_match_method', 'match_method'),
        Index('idx_match_confidence', 'confidence_score'),
        Index('idx_match_verified', 'verified'),
        Index('idx_match_unique', 'applicant_id', 'company_id', unique=True),
    )
    
    # Relationships
    applicant = relationship("Applicant", back_populates="company_matches")
    company = relationship("Company", back_populates="company_matches")

class Contact(Base):
    """Contact information for companies, officers, and applicants"""
    __tablename__ = 'contacts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys (only one should be set per record)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'))
    officer_id = Column(Integer, ForeignKey('officers.id', ondelete='CASCADE'))
    applicant_id = Column(Integer, ForeignKey('applicants.id', ondelete='CASCADE'))
    
    contact_type = Column(String(50), nullable=False)  # email, phone, linkedin, website, etc.
    contact_value = Column(String(500), nullable=False)
    source = Column(String(100))  # companies_house, hunter, clearbit, manual, etc.
    verification_status = Column(String(50), default='unverified')  # verified, unverified, invalid
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_contact_company', 'company_id'),
        Index('idx_contact_officer', 'officer_id'),
        Index('idx_contact_applicant', 'applicant_id'),
        Index('idx_contact_type', 'contact_type'),
        Index('idx_contact_source', 'source'),
        Index('idx_contact_status', 'verification_status'),
    )
    
    # Relationships
    company = relationship("Company", foreign_keys=[company_id], back_populates="contacts")
    officer = relationship("Officer", foreign_keys=[officer_id], back_populates="contacts")
    applicant = relationship("Applicant", foreign_keys=[applicant_id])

class SharedOfficerEdge(Base):
    """Precomputed edges between companies that share officers"""
    __tablename__ = 'shared_officer_edges'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_a_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    company_b_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    shared_officer_count = Column(Integer, default=0)
    last_computed = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_shared_edge_companies', 'company_a_id', 'company_b_id', unique=True),
        Index('idx_shared_edge_count', 'shared_officer_count'),
        Index('idx_shared_edge_computed', 'last_computed'),
    )

# Keep existing tables with enhancements

class EnrichmentData(Base):
    """Enrichment data from various providers"""
    __tablename__ = 'enrichment_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    provider = Column(String(100), nullable=False)
    enrichment_data = Column(JSON)
    success = Column(Boolean, default=True)
    error_message = Column(Text)
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_enrichment_company', 'company_id'),
        Index('idx_enrichment_provider', 'provider'),
        Index('idx_enrichment_success', 'success'),
        Index('idx_enrichment_unique', 'company_id', 'provider', unique=True),
    )
    
    # Relationships
    company = relationship("Company", back_populates="enrichment_data")

class ProcessingLog(Base):
    """Processing log for tracking operations"""
    __tablename__ = 'processing_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    action = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    message = Column(Text)
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_processing_company', 'company_id'),
        Index('idx_processing_action', 'action'),
        Index('idx_processing_status', 'status'),
        Index('idx_processing_created', 'created_at'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="processing_logs")

class LinkedHelperConnection(Base):
    """LinkedIn connections from LinkedHelper"""
    __tablename__ = 'linkedhelper_connections'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(200))
    first_name = Column(String(100))
    last_name = Column(String(100))
    company = Column(String(200))
    position = Column(String(200))
    linkedin_url = Column(Text)
    connection_status = Column(String(50))
    date_connected = Column(DateTime)
    message_sent = Column(Text)
    replied = Column(String(10))
    tags = Column(Text)
    notes = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_linkedin_name', 'full_name'),
        Index('idx_linkedin_company', 'company'),
        Index('idx_linkedin_status', 'connection_status'),
        Index('idx_linkedin_connected', 'date_connected'),
    )

class PlanningData(Base):
    """Legacy planning data table - keeping for migration"""
    __tablename__ = 'planning_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), nullable=False)
    application_type = Column(String(100))
    decision_date = Column(DateTime)
    name = Column(String(200))
    reference = Column(String(50))
    description = Column(Text)
    start_date = Column(DateTime)
    organisation = Column(String(200))
    status = Column(String(50))
    point = Column(String(100))
    planning_url = Column(Text)
    last_updated = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_planning_data_company', 'company_id'),
        Index('idx_planning_data_reference', 'reference'),
        Index('idx_planning_data_status', 'status'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="planning_data")

class AutomationConfig(Base):
    """Configuration settings for automation scheduler"""
    __tablename__ = 'automation_config'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    config_key = Column(String(100), unique=True, nullable=False)
    config_value = Column(Text)
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_automation_config_key', 'config_key', unique=True),
    )

class AutomationRun(Base):
    """Tracks each automated run of the planning application pipeline"""
    __tablename__ = 'automation_runs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_type = Column(String(50), nullable=False)  # 'weekly_scheduled', 'manual', 'retry'
    status = Column(String(50), nullable=False)  # 'running', 'completed', 'failed', 'partial'
    
    # Execution timing
    started_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Processing statistics
    applications_discovered = Column(Integer, default=0)
    applications_processed = Column(Integer, default=0)
    companies_matched = Column(Integer, default=0)
    companies_enriched = Column(Integer, default=0)
    new_companies_created = Column(Integer, default=0)
    new_officers_created = Column(Integer, default=0)
    contacts_enriched = Column(Integer, default=0)
    
    # Error tracking
    error_count = Column(Integer, default=0)
    error_details = Column(Text)
    
    # Configuration used for this run
    boroughs_processed = Column(ARRAY(String))
    date_range_start = Column(DateTime)
    date_range_end = Column(DateTime)
    
    # Detailed logs and metadata
    processing_log = Column(JSON)
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_automation_run_status', 'status'),
        Index('idx_automation_run_type', 'run_type'),
        Index('idx_automation_run_started', 'started_at'),
        Index('idx_automation_run_completed', 'completed_at'),
    )

class AutomationSchedule(Base):
    """Scheduled automation jobs configuration"""
    __tablename__ = 'automation_schedules'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(100), unique=True, nullable=False)
    job_name = Column(String(200), nullable=False)
    
    # Schedule configuration
    schedule_type = Column(String(50), nullable=False)  # 'cron', 'interval'
    cron_expression = Column(String(100))  # For cron-based schedules
    interval_seconds = Column(Integer)  # For interval-based schedules
    
    # Job configuration
    is_enabled = Column(Boolean, default=True)
    job_config = Column(JSON)  # Store job-specific configuration
    
    # Execution tracking
    last_run_id = Column(Integer, ForeignKey('automation_runs.id'))
    next_execution = Column(DateTime)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_automation_schedule_job_id', 'job_id', unique=True),
        Index('idx_automation_schedule_enabled', 'is_enabled'),
        Index('idx_automation_schedule_next', 'next_execution'),
    )
    
    # Relationships
    last_run = relationship("AutomationRun", foreign_keys=[last_run_id])