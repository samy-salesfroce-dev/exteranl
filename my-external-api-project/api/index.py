# api/index.py
import os
import json
import psycopg2
from flask import Flask, request, jsonify
from urllib.parse import unquote_plus # Use unquote_plus for URL decoding
import logging # Import logging module

app = Flask(__name__)

# Configure logging for better debugging on Vercel
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IMPORTANT: This variable MUST be set in Vercel project settings as an Environment Variable.
# NEVER hardcode your database URL here, especially with credentials.
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")

def get_db_connection():
    """Establishes and returns a database connection."""
    if not NEON_DATABASE_URL:
        logger.error("NEON_DATABASE_URL environment variable is not set. Cannot connect to database.")
        raise ValueError("Database connection string (NEON_DATABASE_URL) is missing.")
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        logger.info("Successfully connected to Neon database.")
        return conn
    except Exception as e:
        logger.exception(f"Error connecting to Neon database: {e}")
        raise

@app.route('/api/odata/<path:entity_set>', methods=['GET'])
def odata_endpoint(entity_set):
    """
    Handles OData-like GET requests for specified entity sets (tables).
    This is a simplified implementation. For a full OData v4 server,
    consider dedicated OData libraries or more robust parsing logic.
    """
    logger.info(f"Received request for entity_set: {entity_set}")
    logger.info(f"Raw query parameters: {request.args}")

    conn = None # Initialize conn to None
    cur = None  # Initialize cur to None

    try:
        # Sanitize entity_set (table name) - IMPORTANT for security
        # Only allow alphanumeric characters and underscores to prevent SQL injection on table name
        safe_entity_set = ''.join(char for char in entity_set if char.isalnum() or char == '_')
        if not safe_entity_set or safe_entity_set != entity_set:
            logger.warning(f"Invalid entity_set requested: {entity_set}. Sanitized to: {safe_entity_set}")
            return jsonify({"error": "Invalid entity set name provided in the URL."}), 400

        # --- Basic OData Query Parameter Parsing ---
        # NOTE: Full OData parsing is complex. This handles common Salesforce Connect needs.
        # For production, you MUST use a robust OData filter parser library
        # or implement very careful, exhaustive sanitization and translation.

        # $select: Comma-separated list of properties to include
        select_param = request.args.get('$select')
        columns_to_select = "*"
        if select_param:
            # Basic sanitization for column names in $select
            # Split by comma, strip whitespace, keep only alphanumeric and underscore
            sanitized_columns = []
            for col in select_param.split(','):
                cleaned_col = ''.join(c for c in col.strip() if c.isalnum() or c == '_')
                if cleaned_col:
                    sanitized_columns.append(cleaned_col)
            
            if sanitized_columns:
                columns_to_select = ", ".join(sanitized_columns)
            else:
                logger.warning(f"No valid columns found in $select parameter after sanitization: {select_param}. Selecting all columns.")
                columns_to_select = "*" # Fallback to all columns if $select is empty or only invalid chars

        query_parts = []
        sql_params = [] # Use a more distinct name for SQL parameters

        # --- Build WHERE clause from $filter ---
        filter_param = request.args.get('$filter')
        if filter_param:
            # WARNING: This is a highly simplified and potentially INSECURE filter parser.
            # It only supports simple 'Property operator Value' (e.g., 'Name eq "Alice"')
            # It does NOT handle complex OData filters (e.g., 'and', 'or', functions, nested expressions).
            # For production, you MUST use a robust OData filter parser library
            # or implement very careful, exhaustive sanitization and translation.
            try:
                # This example attempts to parse a simple "PropertyName operator 'Value'"
                # Example: "Name eq 'Alice'" or "Id gt 5"
                # This is still very fragile and assumes specific formats.
                parts = filter_param.split(' ')
                if len(parts) >= 3:
                    prop_name_raw = parts[0]
                    operator_raw = parts[1]
                    value_raw = ' '.join(parts[2:])

                    # Sanitize property name (column name)
                    prop_name = ''.join(c for c in prop_name_raw if c.isalnum() or c == '_')
                    
                    if not prop_name:
                        logger.warning(f"Sanitized property name is empty from '{prop_name_raw}'. Ignoring filter part.")
                    elif operator_raw.lower() in ['eq', 'gt', 'lt', 'ge', 'le', 'ne']:
                        # Determine SQL operator
                        sql_operator_map = {
                            'eq': '=', 'gt': '>', 'lt': '<', 'ge': '>=', 'le': '<=', 'ne': '!='
                        }
                        sql_operator = sql_operator_map.get(operator_raw.lower(), '=') # Default to '='

                        # Attempt to parse value
                        value = None
                        if value_raw.startswith("'") and value_raw.endswith("'"):
                            # String value, unquote and store for parameterized query
                            value = unquote_plus(value_raw[1:-1]) # Use unquote_plus here
                        elif value_raw.lower() in ['true', 'false']:
                            value = (value_raw.lower() == 'true')
                        else:
                            try:
                                value = int(value_raw)
                            except ValueError:
                                try:
                                    value = float(value_raw)
                                except ValueError:
                                    value = value_raw # Treat as string if no other type matches

                        query_parts.append(f"{prop_name} {sql_operator} %s")
                        sql_params.append(value)
                    else:
                        logger.warning(f"Unsupported operator in $filter: '{operator_raw}'. Ignoring filter.")
                else:
                    logger.warning(f"Malformed $filter format: '{filter_param}'. Expected 'Property operator Value'. Ignoring filter.")
            except Exception as e:
                logger.error(f"Error processing $filter '{filter_param}': {e}")
                # Do not re-raise, attempt to continue without this filter

        where_clause = ""
        if query_parts:
            where_clause = f" WHERE {' AND '.join(query_parts)}" # Simple AND for multiple conditions if any

        # --- Build ORDER BY clause ---
        orderby_param = request.args.get('$orderby')
        order_by_clause = ""
        if orderby_param:
            order_parts = []
            for part in orderby_param.split(','):
                part = part.strip()
                if part:
                    components = part.split(' ')
                    col_name = ''.join(c for c in components[0] if c.isalnum() or c == '_') # Sanitize column name
                    direction = 'ASC'
                    if len(components) > 1 and components[1].lower() == 'desc':
                        direction = 'DESC'
                    if col_name: # Only add if column name is valid after sanitization
                        order_parts.append(f"{col_name} {direction}")
            if order_parts:
                order_by_clause = f" ORDER BY {', '.join(order_parts)}"
            else:
                logger.warning(f"No valid orderby columns found after sanitization: {orderby_param}. Ignoring orderby.")


        # --- Build LIMIT and OFFSET clauses from $top and $skip ---
        top_param = request.args.get('$top')
        limit_clause = ""
        if top_param:
            try:
                limit_clause = f" LIMIT %s"
                sql_params.append(int(top_param))
            except ValueError:
                logger.warning(f"Invalid $top value: {top_param}. Ignoring limit.")

        skip_param = request.args.get('$skip')
        offset_clause = ""
        if skip_param:
            try:
                offset_clause = f" OFFSET %s"
                sql_params.append(int(skip_param))
            except ValueError:
                logger.warning(f"Invalid $skip value: {skip_param}. Ignoring offset.")

        # Construct the full SQL query
        final_query = f"SELECT {columns_to_select} FROM {safe_entity_set}{where_clause}{order_by_clause}{limit_clause}{offset_clause};"
        logger.info(f"Final SQL query: {final_query}")
        logger.info(f"SQL Parameters: {sql_params}")

        conn = get_db_connection()
        cur = conn.cursor()

        # Execute the query with parameterized values
        cur.execute(final_query, sql_params)
        rows = cur.fetchall()

        # Get column names for building the OData response
        column_names = [desc[0] for desc in cur.description]

        # Format data as list of dictionaries for OData JSON
        data = []
        for row in rows:
            row_dict = {}
            for i, col_value in enumerate(row):
                # Basic type handling for OData compliance (e.g., datetime objects)
                if isinstance(col_value, (type(None))):
                    row_dict[column_names[i]] = None
                elif hasattr(col_value, 'isoformat'): # Handles datetime, date, time objects
                    row_dict[column_names[i]] = col_value.isoformat()
                else:
                    row_dict[column_names[i]] = col_value
            data.append(row_dict)

        # Construct OData v4 JSON response
        odata_response = {
            "@odata.context": f"/api/odata/$metadata#{safe_entity_set}",
            "value": data
        }
        logger.info(f"Successfully retrieved {len(data)} records for {safe_entity_set}.")
        return jsonify(odata_response)

    except ValueError as ve:
        logger.error(f"Client-side or configuration error: {ve}")
        return jsonify({"error": str(ve)}), 400
    except psycopg2.Error as pg_err:
        logger.exception(f"Database error during query execution: {pg_err}")
        return jsonify({"error": f"Database error: {pg_err.pgerror}", "sqlstate": pg_err.pgcode}), 500
    except Exception as e:
        logger.exception(f"An unexpected server error occurred: {e}")
        return jsonify({"error": f"An unexpected server error occurred: {e}"}), 500
    finally:
        # Ensure database connection and cursor are closed even if errors occur
        if cur: # Check if cur object was successfully created
            cur.close()
        if conn: # Check if conn object was successfully created
            conn.close()
            logger.info("Database connection closed.")


# For local development:
# IMPORTANT: This block should NOT run when deployed on Vercel.
# It's only for running 'python api/index.py' on your local machine.
if __name__ == '__main__':
    # >>> THIS LINE HAS BEEN REMOVED TO PREVENT DEPLOYMENT ISSUES AND SECURITY RISKS <<<
    # For local testing, you MUST set the NEON_DATABASE_URL environment variable
    # in your local environment (e.g., via a .env file loaded by python-dotenv,
    # or by exporting it in your terminal before running the script).
    
    if not os.getenv("NEON_DATABASE_URL"):
        logger.warning("NEON_DATABASE_URL environment variable is not set locally. "
                       "Please set it for local testing or it will fail.")
    app.run(debug=True, port=5000)
