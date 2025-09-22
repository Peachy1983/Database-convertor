# Overview

This is a UK Company Data Enrichment Platform that fetches company data from the Companies House API and enriches it with additional data from multiple third-party providers. The application allows users to search for UK companies, retrieve their official registration data, and augment it with contact information, business intelligence, and other enrichment data from services like Apollo, Clearbit, RocketReach, and Lusha.

**🎉 Recently Migrated to PostgreSQL** (September 2025)
- Migrated from SQLite to PostgreSQL for improved performance and scalability
- New comprehensive schema supporting planning applications and officer networks
- All existing data preserved and functionality maintained

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Frontend Architecture
The application uses Streamlit as the web framework, providing an interactive dashboard interface. The frontend is organized as a single-page application with sidebar configuration panels for API keys and main content areas for company search, data display, and bulk processing features. Plotly is integrated for data visualization and charts.

## Backend Architecture
The system follows a modular architecture with clear separation of concerns:

- **API Client Layer**: Dedicated client classes for each external API (Companies House, Apollo, Clearbit, RocketReach, Lusha) with built-in rate limiting and error handling
- **Data Enrichment Manager**: Orchestrates multiple data providers and manages concurrent enrichment requests using ThreadPoolExecutor
- **Database Layer**: SQLite-based persistence with dedicated DatabaseManager class handling all database operations
- **Utility Layer**: Common functions for data validation, formatting, and export operations

## Data Storage Solution
**PostgreSQL Database** with comprehensive schema designed for developer-lender intelligence:

### Core Tables:
- **Companies**: Enhanced with normalized address fields, SIC codes array, proper indexing
- **Planning Applications**: Borough, reference, type, status, location coordinates, URLs  
- **Applicants**: Planning application participants with contact information
- **Officers**: Companies House officers with date of birth, nationality, addresses
- **Appointments**: Officer-company relationships with active status tracking
- **Applicant Company Matches**: ML-powered matching between applicants and registered companies
- **Contacts**: Multi-entity contact information with verification status
- **Shared Officer Edges**: Precomputed company networks via shared officers

### Legacy Tables (Preserved):
- **Enrichment Data**: Multi-provider enrichment results with JSON storage
- **Processing Log**: Audit trail of all database operations
- **LinkedHelper Connections**: LinkedIn outreach campaign tracking
- **Planning Data**: Legacy planning information (being migrated to new structure)

### Key Features:
- **Proper relationships** with foreign key constraints and cascading deletes
- **Performance indexes** on all query patterns and business keys  
- **Idempotent upserts** using PostgreSQL ON CONFLICT for data integrity
- **Connection pooling** for concurrent request handling
- **Session management** via SQLAlchemy context managers

## Authentication and Authorization
The application uses API key-based authentication for all external services. API keys are managed through environment variables and Streamlit's session state, with secure input fields (password type) in the UI for configuration.

## Rate Limiting and Error Handling
Built-in rate limiting for all API clients respects provider limits (e.g., 100 requests/minute for Companies House). Comprehensive error handling includes retry mechanisms for rate limit errors and graceful degradation when services are unavailable.

# External Dependencies

## Primary APIs
- **Companies House API**: Official UK government API for company registration data, requiring free API key
- **Apollo API**: B2B contact and company enrichment service
- **Clearbit API**: Business intelligence and company data enrichment
- **RocketReach API**: Contact information and social media data
- **Lusha API**: Contact enrichment and lead generation

## Core Libraries
- **Streamlit**: Web application framework for the user interface
- **Pandas**: Data manipulation and analysis
- **Plotly**: Interactive data visualization and charting
- **SQLite3**: Embedded database for local data persistence
- **Requests**: HTTP client library for API interactions
- **OpenPyXL**: Excel file generation and export functionality

## Additional Tools
- **ThreadPoolExecutor**: Concurrent processing for bulk enrichment operations
- **JSON**: Data serialization for storing API responses
- **Regular Expressions**: Company number validation and data cleaning