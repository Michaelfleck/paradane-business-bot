from __future__ import annotations

"""
Simple HTML renderer utilities for the Business Reporting module.

- render_template: Replace placeholders of form {KEY} with stringified values.
- render_list_block: Duplicate a <li> line containing {PREFIX[INDEX]_...} for each item or remove it if empty.
"""

from typing import Dict, List
import re


def render_template(template_html: str, context: Dict[str, object]) -> str:
    """
    Replace placeholders in the form {KEY} with the string value of context[KEY].
    Missing keys remain unchanged to allow subsequent passes if needed.

    Args:
        template_html: Raw HTML template as a string.
        context: Dictionary of placeholder values.

    Returns:
        Rendered HTML with placeholders substituted.
    """
    if not context:
        return template_html

    def replace(match: re.Match) -> str:
        key = match.group(1)
        if key in context and context[key] is not None:
            return str(context[key])
        return match.group(0)

    # Match placeholders like {BUSINESS_NAME}, capturing KEY without braces
    pattern = re.compile(r"\{([A-Z0-9_\[\]\:]+)\}")
    return pattern.sub(replace, template_html)


def render_list_block(template_html: str, key_prefix: str, items: List[str]) -> str:
    """
    Find the line containing the first occurrence of a placeholder that matches
    {key_prefix[INDEX]_...} pattern and duplicate that entire <li> line
    for each item, replacing the placeholder with the item string.

    Specific to our template we target lines like:
        <li><b>Web-Page:</b> <a href="{BUSINESS_CONTACT_PAGE[INDEX]_URL}">{BUSINESS_CONTACT_PAGE[INDEX]_URL}</a></li>

    Rules:
    - If items is empty, remove that entire line.
    - If multiple placeholders exist in the same line (href and text), replace both with the same item value.

    Args:
        template_html: Raw HTML string.
        key_prefix: Prefix like "BUSINESS_CONTACT_PAGE".
        items: List of string items to insert.

    Returns:
        HTML with the list block expanded or removed.
    """
    lines = template_html.splitlines(keepends=False)
    # Build regex to find placeholders like {KEY_PREFIX[INDEX]_SOMETHING}
    placeholder_re = re.compile(r"\{(" + re.escape(key_prefix) + r"\[INDEX\]_[A-Z0-9_]+)\}")
    line_index = None

    for idx, line in enumerate(lines):
        if placeholder_re.search(line):
            line_index = idx
            break

    if line_index is None:
        return template_html  # nothing to do

    template_line = lines[line_index]

    if not items:
        # remove the line entirely
        del lines[line_index]
        return "\n".join(lines)

    expanded_lines: List[str] = []
    for item in items:
        # Replace all matching placeholders on the line with the item text
        def _rep(m: re.Match) -> str:
            return item

        expanded_line = placeholder_re.sub(_rep, template_line)
        expanded_lines.append(expanded_line)

    # Replace the single template line with many
    lines = lines[:line_index] + expanded_lines + lines[line_index + 1 :]
    return "\n".join(lines)