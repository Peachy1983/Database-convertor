"""
Applicant processing module for the developer-lender intelligence system.
Handles fuzzy string matching between planning applicants and Companies House companies.
"""
import re
import difflib
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from dataclasses import dataclass
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

@dataclass
class CompanyMatch:
    """Represents a potential match between an applicant and a company"""
    company_id: int
    company_number: str
    company_name: str
    match_method: str
    confidence_score: float
    applicant_name: str
    normalized_applicant_name: str

class ApplicantProcessor:
    """Processes planning applicants and matches them to Companies House companies"""
    
    def __init__(self):
        # Common company suffixes and their variations
        self.company_suffixes = {
            'limited': ['ltd', 'ltd.', 'limited'],
            'company': ['co', 'co.', 'company'],
            'corporation': ['corp', 'corp.', 'corporation'],
            'incorporated': ['inc', 'inc.', 'incorporated'],
            'partnership': ['partnership', 'partners'],
            'llp': ['llp', 'l.l.p.', 'limited liability partnership'],
            'plc': ['plc', 'p.l.c.', 'public limited company'],
            'cic': ['cic', 'c.i.c.', 'community interest company'],
            'holdings': ['holdings', 'holding'],
            'group': ['group', 'grp'],
            'developments': ['developments', 'development', 'dev'],
            'properties': ['properties', 'property', 'prop'],
            'investments': ['investments', 'investment', 'inv'],
            'services': ['services', 'service', 'svc'],
            'solutions': ['solutions', 'solution', 'sol'],
            'enterprises': ['enterprises', 'enterprise', 'ent'],
            'trading': ['trading', 'trade'],
            'residential': ['residential', 'resi'],
            'commercial': ['commercial', 'comm']
        }
        
        # Words to remove for better matching
        self.stop_words = {
            'the', 'a', 'an', 'and', 'or', 'of', 'in', 'at', 'to', 'for', 'with',
            'by', 'from', 'on', 'as', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'having'
        }
        
        # Common individual titles to identify personal applicants
        self.individual_titles = {
            'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'sir', 'dame', 'lord',
            'lady', 'hon', 'rev', 'captain', 'major', 'colonel'
        }
    
    @lru_cache(maxsize=1000)
    def normalize_company_name(self, name: str) -> str:
        """Normalize company name for better matching"""
        if not name:
            return ""
        
        # Convert to lowercase and strip
        normalized = name.lower().strip()
        
        # Remove common punctuation but keep hyphens and apostrophes
        normalized = re.sub(r'[^\w\s\'-]', ' ', normalized)
        
        # Replace multiple spaces with single space
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove leading/trailing whitespace
        normalized = normalized.strip()
        
        return normalized
    
    def remove_company_suffixes(self, name: str) -> str:
        """Remove common company suffixes for better matching"""
        normalized = self.normalize_company_name(name)
        words = normalized.split()
        
        # Check for suffixes at the end
        for suffix_group, variations in self.company_suffixes.items():
            for variation in variations:
                if words and words[-1] == variation:
                    words = words[:-1]
                    break
                # Also check for suffix with preceding 'and'
                if len(words) >= 2 and words[-2:] == ['and', variation]:
                    words = words[:-2]
                    break
        
        return ' '.join(words).strip()
    
    def is_likely_individual(self, name: str) -> bool:
        """Determine if the name is likely an individual rather than a company"""
        normalized = self.normalize_company_name(name)
        words = normalized.split()
        
        # Check for individual titles
        if words and words[0] in self.individual_titles:
            return True
        
        # Check for common individual patterns
        # Simple heuristics: if it's 2-3 words and no company suffixes
        has_company_suffix = False
        for suffix_group, variations in self.company_suffixes.items():
            if any(variation in normalized for variation in variations):
                has_company_suffix = True
                break
        
        # If no company suffix and 2-3 words, likely individual
        if not has_company_suffix and 2 <= len(words) <= 3:
            return True
        
        return False
    
    def extract_name_tokens(self, name: str) -> set:
        """Extract meaningful tokens from company name"""
        normalized = self.remove_company_suffixes(name)
        words = normalized.split()
        
        # Filter out stop words and very short words
        tokens = {
            word for word in words 
            if len(word) > 2 and word not in self.stop_words
        }
        
        return tokens
    
    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def calculate_string_similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity score between two strings (0-1)"""
        if not s1 or not s2:
            return 0.0
        
        # Normalize both strings
        norm_s1 = self.normalize_company_name(s1)
        norm_s2 = self.normalize_company_name(s2)
        
        if norm_s1 == norm_s2:
            return 1.0
        
        # Use difflib for quick similarity
        similarity = difflib.SequenceMatcher(None, norm_s1, norm_s2).ratio()
        
        return similarity
    
    def calculate_token_similarity(self, applicant_name: str, company_name: str) -> float:
        """Calculate token-based similarity score"""
        applicant_tokens = self.extract_name_tokens(applicant_name)
        company_tokens = self.extract_name_tokens(company_name)
        
        if not applicant_tokens or not company_tokens:
            return 0.0
        
        # Calculate Jaccard similarity
        intersection = len(applicant_tokens & company_tokens)
        union = len(applicant_tokens | company_tokens)
        
        if union == 0:
            return 0.0
        
        jaccard_similarity = intersection / union
        
        # Boost score if all applicant tokens are found in company tokens
        if applicant_tokens.issubset(company_tokens):
            jaccard_similarity = min(1.0, jaccard_similarity + 0.2)
        
        return jaccard_similarity
    
    def find_potential_matches(self, applicant_name: str, companies: List[Dict]) -> List[CompanyMatch]:
        """Find potential company matches for an applicant name"""
        matches = []
        
        # Skip if likely individual
        if self.is_likely_individual(applicant_name):
            logger.debug(f"Skipping individual applicant: {applicant_name}")
            return matches
        
        normalized_applicant = self.normalize_company_name(applicant_name)
        
        for company in companies:
            company_id = company.get('id')
            company_number = company.get('company_number', '')
            company_name = company.get('company_name', '')
            
            if not company_name:
                continue
            
            # Calculate different similarity scores
            string_similarity = self.calculate_string_similarity(applicant_name, company_name)
            token_similarity = self.calculate_token_similarity(applicant_name, company_name)
            
            # Check for exact match after suffix removal
            applicant_no_suffix = self.remove_company_suffixes(applicant_name)
            company_no_suffix = self.remove_company_suffixes(company_name)
            
            if applicant_no_suffix and company_no_suffix:
                suffix_similarity = self.calculate_string_similarity(applicant_no_suffix, company_no_suffix)
            else:
                suffix_similarity = 0.0
            
            # Determine best match method and score
            if string_similarity >= 0.95:
                match_method = "exact_name"
                confidence_score = string_similarity
            elif suffix_similarity >= 0.9:
                match_method = "suffix_normalized"
                confidence_score = suffix_similarity
            elif token_similarity >= 0.7:
                match_method = "token_match"
                confidence_score = token_similarity
            elif string_similarity >= 0.8:
                match_method = "fuzzy_name"
                confidence_score = string_similarity
            else:
                # Skip low confidence matches
                continue
            
            # Create match object
            match = CompanyMatch(
                company_id=company_id,
                company_number=company_number,
                company_name=company_name,
                match_method=match_method,
                confidence_score=confidence_score,
                applicant_name=applicant_name,
                normalized_applicant_name=normalized_applicant
            )
            
            matches.append(match)
        
        # Sort by confidence score (highest first)
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        # Return top 5 matches
        return matches[:5]
    
    def validate_applicant_data(self, applicant_data: Dict) -> Tuple[bool, str]:
        """Validate incoming applicant data format"""
        required_fields = ['planning_reference', 'applicant_name']
        
        for field in required_fields:
            if field not in applicant_data:
                return False, f"Missing required field: {field}"
            
            if not applicant_data[field] or not str(applicant_data[field]).strip():
                return False, f"Empty required field: {field}"
        
        # Validate planning reference format
        planning_ref = str(applicant_data['planning_reference']).strip()
        if len(planning_ref) < 3:
            return False, "Planning reference too short"
        
        # Validate applicant name
        applicant_name = str(applicant_data['applicant_name']).strip()
        if len(applicant_name) < 2:
            return False, "Applicant name too short"
        
        return True, "Valid"
    
    def normalize_applicant_data(self, applicant_data: Dict) -> Dict:
        """Normalize applicant data for consistent processing"""
        normalized = {}
        
        # Copy and normalize basic fields
        normalized['planning_reference'] = str(applicant_data['planning_reference']).strip().upper()
        normalized['raw_name'] = str(applicant_data['applicant_name']).strip()
        normalized['normalized_name'] = self.normalize_company_name(normalized['raw_name'])
        
        # Optional fields
        normalized['borough'] = applicant_data.get('borough', '').strip()
        normalized['contact_email'] = applicant_data.get('contact_email', '').strip()
        normalized['contact_phone'] = applicant_data.get('contact_phone', '').strip()
        normalized['contact_address'] = applicant_data.get('contact_address', '').strip()
        
        # Determine applicant type
        if self.is_likely_individual(normalized['raw_name']):
            normalized['applicant_type'] = 'individual'
        else:
            normalized['applicant_type'] = 'company'
        
        # Add processing metadata
        normalized['processed_at'] = datetime.now()
        
        return normalized
    
    def deduplicate_applicants(self, applicants: List[Dict]) -> List[Dict]:
        """Remove duplicate applicant records"""
        seen = set()
        deduplicated = []
        
        for applicant in applicants:
            # Create a deduplication key
            key = (
                applicant.get('planning_reference', '').upper().strip(),
                self.normalize_company_name(applicant.get('applicant_name', ''))
            )
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(applicant)
            else:
                logger.debug(f"Duplicate applicant found: {applicant.get('applicant_name')} for {applicant.get('planning_reference')}")
        
        logger.info(f"Deduplicated {len(applicants)} applicants to {len(deduplicated)} unique records")
        return deduplicated