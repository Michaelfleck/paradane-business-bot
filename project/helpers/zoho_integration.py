import logging
from typing import Dict, Any, List, Optional, Tuple
from project.libs.zoho_client import get_zoho_client
from project.libs.supabase_client import get_client
import re
import tempfile
import os
import mimetypes
import requests

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

def parse_social_links(social_links: str) -> Dict[str, str]:
    """Parse social links string into a dictionary."""
    links = {}
    if not social_links:
        return links

    for item in social_links.split(','):
        item = item.strip()
        if ':' in item:
            key, value = item.split(':', 1)
            links[key.strip()] = value.strip()

    return links

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

    # Parse social links to extract Twitter
    social_links = parse_social_links(business.get('social_links', ''))
    twitter = social_links.get('twitter')

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
        'Twitter': twitter,
        'Street': address_data.get('street'),
        'City': address_data.get('city'),
        'State': address_data.get('state'),
        'Zip_Code': address_data.get('zip_code'),
        'Country': address_data.get('country'),
    }

    # Remove None values to avoid sending empty fields
    return {k: v for k, v in lead_data.items() if v is not None}

def map_business_to_account(business: Dict[str, Any]) -> Dict[str, Any]:
    """Map business data to Zoho Account fields."""

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

    account_data = {
        'Account_Name': business.get('name'),
        'Phone': business.get('phone'),
        'Website': website,
        'Billing_Street': address_data.get('street'),
        'Billing_City': address_data.get('city'),
        'Billing_State': address_data.get('state'),
        'Billing_Code': address_data.get('zip_code'),
        'Billing_Country': address_data.get('country'),
    }

    # Remove None values to avoid sending empty fields
    return {k: v for k, v in account_data.items() if v is not None}

def map_lead_to_account(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Map lead data to Zoho Account fields."""

    account_data = {
        'Account_Name': lead.get('Company'),
        'Phone': lead.get('Phone'),
        'Website': lead.get('Website'),
        'Billing_Street': lead.get('Street'),
        'Billing_City': lead.get('City'),
        'Billing_State': lead.get('State'),
        'Billing_Code': lead.get('Zip_Code'),
        'Billing_Country': lead.get('Country'),
    }

    # Remove None values to avoid sending empty fields
    return {k: v for k, v in account_data.items() if v is not None}

def create_zoho_lead_for_business(business: Dict[str, Any]) -> Optional[str]:
    """Create a Zoho lead for a business and return the lead ID. Checks for duplicates first."""
    try:
        client = get_zoho_client()
        company_name = business.get('name')
        logger.info(f"Creating Zoho lead for business {business['id']}, company_name: '{company_name}'")

        # Check for existing lead with same company name
        if company_name:
            existing_leads = client.search_leads({"Company": company_name})
            logger.info(f"Searched for existing leads with Company: '{company_name}', found: {len(existing_leads)}")
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

        # Create or link account
        account_id = None
        company_name = business.get('name')
        if company_name:
            # Check for existing account with same company name
            existing_accounts = client.search_accounts({"Account_Name": company_name})
            logger.info(f"Searched for existing accounts with Account_Name: '{company_name}', found: {len(existing_accounts)}")
            if existing_accounts:
                account_id = existing_accounts[0]['id']
                logger.info(f"Found existing Zoho account {account_id} for business {business['id']} (company: {company_name})")
            else:
                # Create new account
                account_data = map_business_to_account(business)
                logger.info(f"Creating new account for business {business['id']} with data: {account_data}")
                try:
                    account_id = client.create_account(account_data)
                    logger.info(f"Created Zoho account {account_id} for business {business['id']}")
                except Exception as e:
                    logger.error(f"Failed to create Zoho account for business {business.get('id')}: {e}")
        else:
            logger.info(f"Skipping account creation for business {business['id']} due to missing company name")

        # Link account to lead if account was found or created
        if account_id:
            try:
                update_data = {'Account': account_id}
                success = client.update_lead(lead_id, update_data)
                logger.info(f"Linked account {account_id} to lead {lead_id}, update success: {success}")
            except Exception as e:
                logger.error(f"Failed to link account {account_id} to lead {lead_id}: {e}")

        # Create note with emails if available
        emails = business.get('emails', [])
        if emails:
            note_content = "Emails: " + ", ".join(emails)
            note_data = {
                'Note_Content': note_content
            }
            try:
                client.create_note("Leads", lead_id, note_data)
                logger.info(f"Added emails note to lead {lead_id}")
            except Exception as e:
                logger.error(f"Failed to add emails note to lead {lead_id}: {e}")

        # Update the business record with the Zoho lead ID
        supabase_client = get_client()
        supabase_client.table("businesses").update({"zoho_lead_id": lead_id}).eq("id", business["id"]).execute()

        logger.info(f"Created Zoho lead {lead_id} for business {business['id']}")
        return lead_id
    except Exception as e:
        logger.error(f"Failed to create Zoho lead for business {business.get('id')}: {e}")
        return None

def update_lead(lead_id: str, lead_data: Dict[str, Any]) -> bool:
    """Update an existing lead in Zoho CRM."""
    try:
        client = get_zoho_client()
        success = client.update_lead(lead_id, lead_data)
        if success:
            logger.info(f"Updated lead {lead_id}")
        return success
    except Exception as e:
        logger.error(f"Failed to update lead {lead_id}: {e}")
        return False

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

        # Retrieve lead to get associated account ID
        lead_details = client.get_lead(lead_id)
        account_id = lead_details.get('Account') if lead_details else None
        company_name = lead_details.get('Company') if lead_details else None
        logger.info(f"Processing contacts for lead {lead_id}, current account_id: {account_id}, company_name: '{company_name}'")
        if not account_id:
            # Create account from lead data and link to lead
            if not company_name:
                logger.info(f"No company name for lead {lead_id}, skipping account creation")
            else:
                account_data = map_lead_to_account(lead_details)
                logger.info(f"Lead has no account, creating account from lead data: {account_data}")
                existing_accounts = client.search_accounts({"Account_Name": company_name})
                logger.info(f"Searched for existing accounts with Account_Name: '{company_name}', found: {len(existing_accounts)}")
                if existing_accounts:
                    account_id = existing_accounts[0]['id']
                    logger.info(f"Using existing account {account_id} for lead {lead_id}")
                else:
                    try:
                        account_id = client.create_account(account_data)
                        logger.info(f"Created Zoho account {account_id} for lead {lead_id}")
                    except Exception as e:
                        logger.error(f"Failed to create account for lead {lead_id}: {e}")
                        # Continue without account
                if account_id:
                    # Link account to lead
                    update_data = {'Account': account_id}
                    success = client.update_lead(lead_id, update_data)
                    if success:
                        logger.info(f"Linked account {account_id} to lead {lead_id}")
                    else:
                        logger.error(f"Failed to link account {account_id} to lead {lead_id}")

        processed_emails = set()

        for email in emails:
            if email in processed_emails:
                continue

            logger.info(f"Processing contact for email {email}, will link to account {account_id}")
            # Check if contact already exists with this email
            existing_contacts = client.search_contacts({"Email": email})
            logger.info(f"Searched for existing contacts with Email: '{email}', found: {len(existing_contacts)}")
            if existing_contacts:
                # Check if contact is already linked to this lead
                contact_id = existing_contacts[0]['id']
                contact_details = client.get_contact(contact_id)
                lead_field = contact_details.get('Lead') if contact_details else None
                if contact_details and lead_field == lead_id:
                    logger.info(f"Contact {contact_id} for email {email} is already linked to lead {lead_id}, skipping update")
                    processed_emails.add(email)
                else:
                    # Update existing contact to link with the lead and account
                    update_data = {'Lead': lead_id}
                    if account_id:
                        update_data['Account_Name'] = account_id
                    logger.info(f"Updating existing contact {contact_id} with data: {update_data}")
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
                if account_id:
                    contact_data['Account_Name'] = account_id  # Link to account during creation

                # Remove None values
                contact_data = {k: v for k, v in contact_data.items() if v is not None}
                logger.info(f"Creating new contact for email {email} with data: {contact_data}")

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

def add_or_update_emails_note(lead_id: str, emails: List[str]) -> bool:
    """Add or update a note with emails on the lead."""
    if not emails:
        return True

    try:
        client = get_zoho_client()
        note_content = "Emails: " + ", ".join(emails)

        # Get existing notes for the lead
        notes = client.get_notes("Leads", lead_id)
        emails_note = None
        for note in notes:
            if note.get('Note_Content', '').startswith('Emails: '):
                emails_note = note
                break

        if emails_note:
            # Update existing note
            note_data = {'Note_Content': note_content}
            success = client.update_note("Leads", lead_id, emails_note['id'], note_data)
            if success:
                logger.info(f"Updated emails note for lead {lead_id}")
            return success
        else:
            # Create new note
            note_data = {'Note_Content': note_content}
            note_id = client.create_note("Leads", lead_id, note_data)
            logger.info(f"Created emails note {note_id} for lead {lead_id}")
            return True
    except Exception as e:
        logger.error(f"Failed to add/update emails note for lead {lead_id}: {e}")
        return False

def check_report_attachment_exists(lead_id: str, report_type: str, business_name: str) -> bool:
    """Check if a report attachment already exists for the lead."""
    try:
        client = get_zoho_client()
        attachments = client.get_attachments("Leads", lead_id)
        expected_name = f"{report_type} - {business_name}.pdf"
        for att in attachments:
            if att.get("File_Name") == expected_name:
                return True
        return False
    except Exception as e:
        logger.warning(f"Failed to check attachments for lead {lead_id}: {e}")
        return False  # On error, assume not exists to allow upload

def check_image_attachment_exists(lead_id: str, business_name: str) -> bool:
    """Check if a business image attachment already exists for the lead."""
    try:
        client = get_zoho_client()
        attachments = client.get_attachments("Leads", lead_id)
        for att in attachments:
            file_name = att.get("File_Name", "")
            if file_name.startswith(f"Business Image - {business_name}"):
                return True
        return False
    except Exception as e:
        logger.warning(f"Failed to check image attachments for lead {lead_id}: {e}")
        return False  # On error, assume not exists to allow upload

def attach_image_to_lead(business_id: str, lead_id: str) -> bool:
    """Attach the business image from image_url to a Zoho lead."""
    logger.info(f"attach_image_to_lead called for business_id={business_id}, lead_id={lead_id}")
    try:
        # Get image_url and business name
        supabase_client = get_client()
        response = supabase_client.table("businesses").select("image_url, name").eq("id", business_id).single().execute()
        if not response.data or not response.data.get("image_url"):
            logger.info(f"No image_url for business {business_id}")
            return False

        image_url = response.data["image_url"]
        business_name = response.data.get("name", "Business")
        logger.info(f"Retrieved image_url='{image_url}' and business_name='{business_name}' for business {business_id}")

        # Note: For photo upload, we don't check existence as photo can be updated

        # Download the image
        logger.info(f"Attempting to download image from {image_url}")
        response_img = requests.get(image_url, timeout=30)
        response_img.raise_for_status()
        logger.info(f"Successfully downloaded image, status={response_img.status_code}, content-length={response_img.headers.get('content-length', 'unknown')}")

        content_type = response_img.headers.get('content-type', 'image/jpeg')
        ext = mimetypes.guess_extension(content_type) or '.jpg'
        file_name = f"Business Image - {business_name}{ext}"
        logger.info(f"Detected content_type='{content_type}', extension='{ext}', file_name='{file_name}'")

        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as temp_file:
            temp_file.write(response_img.content)
            temp_file_path = temp_file.name
        logger.info(f"Saved image to temp file: {temp_file_path}, size={len(response_img.content)} bytes")

        try:
            # Attach to Zoho lead
            logger.info(f"Attempting to attach document to Zoho lead {lead_id}: file_name='{file_name}', content_type='{content_type}'")
            client = get_zoho_client()
            success = client.upload_photo("Leads", lead_id, temp_file_path, content_type)
            if success:
                logger.info(f"Successfully uploaded photo to lead {lead_id} for business {business_id}")
            else:
                logger.error(f"Zoho upload_photo returned False for lead {lead_id}")
            return success
        finally:
            # Clean up temp file
            os.unlink(temp_file_path)
            logger.info(f"Cleaned up temp file: {temp_file_path}")

    except Exception as e:
        logger.error(f"Failed to attach image to lead {lead_id} for business {business_id}: {e}")
        return False