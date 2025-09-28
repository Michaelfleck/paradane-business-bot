import logging
from typing import Dict, Any, List, Optional, Tuple
from project.libs.zoho_client import get_zoho_client
from project.libs.supabase_client import get_client
import re

logger = logging.getLogger(__name__)

def parse_address(formatted_address: str) -> Dict[str, str]:
    """Parse formatted address into components."""
    if not formatted_address:
        return {}

    # Remove extra whitespace and normalize
    address = re.sub(r'\s+', ' ', formatted_address.strip())

    # Try to match common patterns
    # Pattern: "street, city, state zip, country"
    match = re.match(r'^(.+?),\s*(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?),\s*(.+)$', address)
    if match:
        return {
            'street': match.group(1).strip(),
            'city': match.group(2).strip(),
            'state': match.group(3).strip(),
            'zip_code': match.group(4).strip(),
            'country': match.group(5).strip()
        }

    # Fallback: split by commas
    parts = [p.strip() for p in address.split(',')]
    if len(parts) >= 4:
        return {
            'street': parts[0],
            'city': parts[1],
            'state': parts[2].split()[0] if parts[2] else '',
            'zip_code': ' '.join(parts[2].split()[1:]) if len(parts[2].split()) > 1 else '',
            'country': parts[3] if len(parts) > 3 else ''
        }

    return {}

def clean_website_url(url: str) -> str:
    """Remove sub-paths from URL to get root domain."""
    if not url:
        return ''

    # Remove protocol
    url = re.sub(r'^https?://', '', url)

    # Remove path and query
    url = re.sub(r'/.*', '', url)

    return f"https://{url}"

def map_business_to_lead(business: Dict[str, Any]) -> Dict[str, Any]:
    """Map business data to Zoho Lead fields according to spec."""

    # Website priority: attributes.menu_url > google_enrichment.website > website
    website = (
        business.get('attributes', {}).get('menu_url') or
        business.get('google_enrichment', {}).get('website') or
        business.get('website')
    )
    website = clean_website_url(website) if website else None

    # Address parsing
    address_data = {}
    if business.get('google_enrichment', {}).get('formatted_address'):
        address_data = parse_address(business['google_enrichment']['formatted_address'])
    elif business.get('formatted_address'):
        address_data = parse_address(business['formatted_address'])
    else:
        # Fallback to location fields
        location = business.get('location', {})
        address_data = {
            'street': location.get('address1', ''),
            'city': location.get('city', ''),
            'state': location.get('state', ''),
            'zip_code': location.get('zip_code', ''),
            'country': location.get('country', '')
        }

    # Description from Google editorial summary
    description = business.get('google_enrichment', {}).get('editorial_summary', {}).get('overview', '')

    lead_data = {
        'First_Name': None,  # Empty as per spec
        'Title': None,  # Empty as per spec
        'Phone': business.get('phone'),
        'Lead_Source': 'Web Research',
        'Industry': None,  # Empty as per spec
        'Annual_Revenue': None,  # Empty as per spec
        'Email_Opt_Out': None,  # Empty as per spec
        'Company': business.get('name'),
        'Last_Name': business.get('name'),
        'Email': None,  # Empty as per spec
        'Fax': None,  # Empty as per spec
        'Website': website,
        'Lead_Status': 'Not Contacted',
        'No_of_Employees': None,  # Empty as per spec
        'Rating': None,  # Empty as per spec
        'Secondary_Email': None,  # Empty as per spec
        'Twitter': None,  # Empty as per spec
        'Street': address_data.get('street'),
        'City': address_data.get('city'),
        'State': address_data.get('state'),
        'Zip_Code': address_data.get('zip_code'),
        'Country': address_data.get('country'),
        'Description': description
    }

    # Remove None values to avoid sending empty fields
    return {k: v for k, v in lead_data.items() if v is not None}

def create_zoho_lead_for_business(business: Dict[str, Any]) -> Optional[str]:
    """Create a Zoho lead for a business and return the lead ID. Checks for duplicates first."""
    try:
        client = get_zoho_client()
        company_name = business.get('name')

        # Check for existing lead with same company name
        if company_name:
            existing_leads = client.search_leads({"Company": company_name})
            if existing_leads:
                existing_lead_id = existing_leads[0]['id']
                logger.info(f"Found existing Zoho lead {existing_lead_id} for business {business['id']} (company: {company_name})")

                # Update the business record with the existing Zoho lead ID
                supabase_client = get_client()
                supabase_client.table("businesses").update({"zoho_lead_id": existing_lead_id}).eq("id", business["id"]).execute()

                return existing_lead_id

        # No existing lead found, create new one
        lead_data = map_business_to_lead(business)
        lead_id = client.create_lead(lead_data)

        # Update the business record with the Zoho lead ID
        supabase_client = get_client()
        supabase_client.table("businesses").update({"zoho_lead_id": lead_id}).eq("id", business["id"]).execute()

        logger.info(f"Created Zoho lead {lead_id} for business {business['id']}")
        return lead_id
    except Exception as e:
        logger.error(f"Failed to create Zoho lead for business {business.get('id')}: {e}")
        return None

def update_lead_with_emails(lead_id: str, emails: List[str]) -> bool:
    """Update Zoho lead with email addresses."""
    if not emails:
        return True

    try:
        client = get_zoho_client()
        # Update the lead with the first email
        update_data = {'Email': emails[0]}
        if len(emails) > 1:
            # Add secondary emails if available (Zoho may have limited fields)
            update_data['Secondary_Email'] = emails[1]

        success = client.update_lead(lead_id, update_data)
        if success:
            logger.info(f"Updated lead {lead_id} with emails: {emails[:2]}")
        return success
    except Exception as e:
        logger.error(f"Failed to update lead {lead_id} with emails: {e}")
        return False

def derive_name_from_email(email: str) -> Tuple[Optional[str], Optional[str]]:
    """Derive first and last name from email address."""
    if not email or '@' not in email:
        return None, None

    local_part = email.split('@')[0].lower()

    # Only handle clear, unambiguous patterns
    patterns = [
        (r'^([a-z]+)\_([a-z]+)$', lambda m: (m.group(1).capitalize(), m.group(2).capitalize())),  # john_doe
        (r'^([a-z]+)\.([a-z]+)$', lambda m: (m.group(1).capitalize(), m.group(2).capitalize())),  # john.doe
        (r'^([a-z])\.([a-z]+)$', lambda m: (m.group(1).upper(), m.group(2).capitalize())),  # j.doe
    ]

    for pattern, extractor in patterns:
        match = re.match(pattern, local_part)
        if match:
            first, last = extractor(match)
            return first, last

    # For ambiguous cases, use as last name only to avoid incorrect assumptions
    return None, local_part.capitalize()

def create_contacts_for_emails(lead_id: str, emails: List[str]) -> bool:
    """Create Zoho contacts for each unique email associated with the lead, or link existing contacts."""
    if not emails:
        return True

    try:
        client = get_zoho_client()
        processed_emails = set()

        for email in emails:
            if email in processed_emails:
                continue

            # Check if contact already exists with this email
            existing_contacts = client.search_contacts({"Email": email})
            if existing_contacts:
                # Check if contact is already linked to this lead
                contact_id = existing_contacts[0]['id']
                contact_details = client.get_contact(contact_id)
                if contact_details and contact_details.get('Lead') == lead_id:
                    logger.info(f"Contact {contact_id} for email {email} is already linked to lead {lead_id}, skipping update")
                    processed_emails.add(email)
                else:
                    # Update existing contact to link with the lead
                    update_data = {'Lead': lead_id}
                    success = client.update_contact(contact_id, update_data)
                    if success:
                        logger.info(f"Linked existing contact {contact_id} for email {email} to lead {lead_id}")
                        processed_emails.add(email)
                    else:
                        logger.error(f"Failed to link existing contact {contact_id} for email {email} to lead {lead_id}")
            else:
                # Create new contact
                first_name, last_name = derive_name_from_email(email)

                contact_data = {
                    'Email': email,
                    'First_Name': first_name,
                    'Last_Name': last_name,
                    'Lead_Source': 'Web Research',
                    'Lead': lead_id  # Link to lead during creation
                }

                # Remove None values
                contact_data = {k: v for k, v in contact_data.items() if v is not None}

                try:
                    contact_id = client.create_contact(contact_data)
                    logger.info(f"Created contact {contact_id} for email {email} linked to lead {lead_id}")
                    processed_emails.add(email)
                except Exception as e:
                    logger.error(f"Failed to create contact for email {email}: {e}")
                    continue

        return True
    except Exception as e:
        logger.error(f"Failed to process contacts for lead {lead_id}: {e}")
        return False

def attach_pdf_to_lead(lead_id: str, pdf_path: str, report_type: str) -> bool:
    """Attach a PDF report to a Zoho lead."""
    try:
        client = get_zoho_client()
        business_name = get_business_name_by_lead_id(lead_id) or "Business"
        file_name = f"{report_type} - {business_name}.pdf"

        success = client.attach_document("Leads", lead_id, pdf_path, file_name)
        if success:
            logger.info(f"Attached {report_type} PDF to lead {lead_id}")
        return success
    except Exception as e:
        logger.error(f"Failed to attach PDF to lead {lead_id}: {e}")
        return False

def get_business_name_by_lead_id(lead_id: str) -> Optional[str]:
    """Get business name by Zoho lead ID."""
    try:
        supabase_client = get_client()
        response = supabase_client.table("businesses").select("name").eq("zoho_lead_id", lead_id).single().execute()
        return response.data.get("name") if response.data else None
    except Exception:
        return None

def get_lead_id_by_business_id(business_id: str) -> Optional[str]:
    """Get Zoho lead ID for a business."""
    try:
        supabase_client = get_client()
        response = supabase_client.table("businesses").select("zoho_lead_id").eq("id", business_id).single().execute()
        return response.data.get("zoho_lead_id") if response.data else None
    except Exception:
        return None