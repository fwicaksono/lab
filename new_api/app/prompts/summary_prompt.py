def billing_summary():
    BILLING_SUMMARY_PROMPT = """
Generate brief, professional explanations for medical billing item types provided in the input.

For each item type, provide a 1-2 sentence explanation of what this category covers in medical billing context.
Return as a JSON array of strings, one explanation per item type in the same order as provided.
Keep explanations concise and focused on billing/cost aspects.

Example format: 
["Explanation for first type", "Explanation for second type", ...]

Please analyze the following item types and provide explanations for each.
"""
    return BILLING_SUMMARY_PROMPT