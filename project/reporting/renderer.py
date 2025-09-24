from __future__ import annotations

"""
Simple HTML renderer utilities for the Business Reporting module.

- render_template: Replace placeholders of form {KEY} with stringified values.
- render_list_block: Duplicate a <li> line containing {PREFIX[INDEX]_...} for each item or remove it if empty.
- render_indexed_line_block: Duplicate a single line that contains an INDEX placeholder and replace
  multiple sibling placeholders for the same INDEX per item (useful for website-report rows).
"""

from typing import Dict, List, Callable
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


def render_indexed_line_block(
    template_html: str,
    match_placeholder: str,
    item_count: int,
    render_for_index: Callable[[int, str], str],
) -> str:
    """
    Duplicate a single template line that contains a given placeholder (e.g., "{BUSINESS_PAGE[INDEX]_URL}")
    for item_count times, using render_for_index(index, line_template) to replace all sibling placeholders
    on that line.

    - match_placeholder: the exact placeholder to locate the line (must be unique in the template).
    - item_count: number of items to render.
    - render_for_index: callback that takes index and the template line, returns replaced line.

    If the match line is not found, returns template unchanged.
    If item_count is zero, removes that line.
    """
    lines = template_html.splitlines(keepends=False)
    line_index = None
    for idx, line in enumerate(lines):
        if match_placeholder in line:
            line_index = idx
            break

    if line_index is None:
        return template_html

    row_template = lines[line_index]
    if item_count <= 0:
        del lines[line_index]
        return "\n".join(lines)

    rendered_rows: List[str] = []
    for i in range(item_count):
        rendered_rows.append(render_for_index(i, row_template))

    lines = lines[:line_index] + rendered_rows + lines[line_index + 1 :]
    return "\n".join(lines)


def render_indexed_block_between(
    template_html: str,
    start_marker: str,
    end_marker: str,
    item_count: int,
    render_for_index: Callable[[int, str], str],
) -> str:
    """
    Duplicate a multi-line block between start_marker and end_marker (inclusive of both lines)
    for each index. The block should contain INDEX placeholders (e.g., {BUSINESS_PAGE[INDEX]_URL}).

    If item_count == 0, the whole block is removed.
    If markers are not found, template is returned unchanged.
    """
    lines = template_html.splitlines(keepends=False)
    start_idx = end_idx = None
    for i, ln in enumerate(lines):
        if start_idx is None and start_marker in ln:
            start_idx = i
        if start_idx is not None and end_marker in ln:
            end_idx = i
            break

    if start_idx is None or end_idx is None or end_idx < start_idx:
        return template_html

    block_lines = lines[start_idx : end_idx + 1]
    block = "\n".join(block_lines)

    if item_count <= 0:
        # remove the block
        new_lines = lines[:start_idx] + lines[end_idx + 1 :]
        return "\n".join(new_lines)

    rendered_blocks: List[str] = []
    for idx in range(item_count):
        rendered_blocks.append(render_for_index(idx, block))

    new_lines = lines[:start_idx] + ["\n".join(rendered_blocks)] + lines[end_idx + 1 :]
    return "\n".join(new_lines)