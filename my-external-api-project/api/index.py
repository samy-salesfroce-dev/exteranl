# api/index.py
import os
import json
import psycopg2
from flask import Flask, request, jsonify, Response
from urllib.parse import unquote_plus
import logging
from psycopg2.extras import Json # Import for handling JSON data if needed

app = Flask(__name__)

# Configure logging for better debugging on Vercel
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# --- OData Metadata Endpoint ---
@app.route('/api/odata/$metadata', methods=['GET'])
def odata_metadata():
    """
    Serves the OData $metadata (CSDL XML) document.
    This XML is now configured for your 'event' table with 'ExternalHistory' entity type.
    """
    logger.info("Received request for OData $metadata endpoint.")

    # EntitySet Name ('event') MUST match your actual PostgreSQL table name.
    # EntityType Name ('ExternalHistory') is a logical name.
    
    metadata_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
    <edmx:DataServices>
        <Schema Namespace="externalapi" xmlns="http://docs.oasis-open.org/odata/ns/edm">
            <EntityType Name="ExternalHistory">
                <Key><PropertyRef Name="id"/></Key>
                <Property Name="id" Type="Edm.Int32" Nullable="false"/>
                <Property Name="Data time" Type="Edm.DateTimeOffset"/>
                <Property Name="Event type" Type="Edm.String"/>
                <Property Name="Field name" Type="Edm.String"/>
                <Property Name="New value" Type="Edm.String"/>
                <Property Name="Object" Type="Edm.String"/>
                <Property Name="Old value" Type="Edm.String"/>
                <Property Name="Record Id" Type="Edm.String"/>
                <Property Name="Salesforce id" Type="Edm.String"/>
                <Property Name="User" Type="Edm.String"/>
            </EntityType>

            <EntityContainer Name="DefaultContainer">
                <EntitySet Name="event" EntityType="externalapi.ExternalHistory"/>
            </EntityContainer>
        </Schema>
    </edmx:DataServices>
</edmx:Edmx>"""

    return Response(metadata_xml, mimetype='application/xml')


# --- OData Endpoint (NOW Handles GET and POST) ---
@app.route('/api/odata/<path:entity_set>', methods=['GET', 'POST'])
def odata_endpoint(entity_set):
    """
    Handles OData-like GET requests for specified entity sets (tables)
    and POST requests for creating new records.
    """
    logger.info(f"Received {request.method} request for entity_set: {entity_set}")
    logger.info(f"Raw query parameters: {request.args}")

    conn = None
    cur = None

    try:
        # Sanitize entity_set (table name)
        safe_entity_set_quoted = f'"{entity_set}"' 
        safe_entity_set_raw = ''.join(char for char in entity_set if char.isalnum() or char == '_')
        if not safe_entity_set_raw or safe_entity_set_raw != entity_set:
            logger.warning(f"Invalid entity_set requested: {entity_set}. Sanitized to: {safe_entity_set_raw}")
            return jsonify({"error": "Invalid entity set name provided in the URL."}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        if request.method == 'GET':
            # --- GET Logic (Existing Code) ---
            select_param = request.args.get('$select')
            columns_to_select = "*"
            if select_param:
                sanitized_columns = []
                for col in select_param.split(','):
                    cleaned_col = col.strip()
                    if cleaned_col:
                        sanitized_columns.append(f'"{cleaned_col}"') # Quote column names for SQL
                if sanitized_columns:
                    columns_to_select = ", ".join(sanitized_columns)
                else:
                    logger.warning(f"No valid columns found in $select parameter after sanitization: {select_param}. Selecting all columns.")
                    columns_to_select = "*"

            query_parts = []
            sql_params = []

            filter_param = request.args.get('$filter')
            if filter_param:
                try:
                    parts = filter_param.split(' ')
                    if len(parts) >= 3:
                        prop_name_raw = parts[0]
                        operator_raw = parts[1]
                        value_raw = ' '.join(parts[2:])

                        prop_name_quoted = f'"{prop_name_raw}"' # Quote property name for SQL
                        
                        if operator_raw.lower() in ['eq', 'gt', 'lt', 'ge', 'le', 'ne']:
                            sql_operator_map = {
                                'eq': '=', 'gt': '>', 'lt': '<', 'ge': '>=', 'le': '<=', 'ne': '!='
                            }
                            sql_operator = sql_operator_map.get(operator_raw.lower(), '=')

                            value = None
                            if value_raw.startswith("'") and value_raw.endswith("'"):
                                value = unquote_plus(value_raw[1:-1])
                            elif value_raw.lower() in ['true', 'false']:
                                value = (value_raw.lower() == 'true')
                            else:
                                try:
                                    value = int(value_raw)
                                except ValueError:
                                    try:
                                        value = float(value_raw)
                                    except ValueError:
                                        value = value_raw

                            query_parts.append(f"{prop_name_quoted} {sql_operator} %s")
                            sql_params.append(value)
                        else:
                            logger.warning(f"Unsupported operator in $filter: '{operator_raw}'. Ignoring filter.")
                    else:
                        logger.warning(f"Malformed $filter format: '{filter_param}'. Expected 'Property operator Value'. Ignoring filter.")
                except Exception as e:
                    logger.error(f"Error processing $filter '{filter_param}': {e}")

            where_clause = ""
            if query_parts:
                where_clause = f" WHERE {' AND '.join(query_parts)}"

            orderby_param = request.args.get('$orderby')
            order_by_clause = ""
            if orderby_param:
                order_parts = []
                for part in orderby_param.split(','):
                    part = part.strip()
                    if part:
                        components = part.split(' ')
                        col_name_raw = components[0]
                        col_name_quoted = f'"{col_name_raw}"'
                        direction = 'ASC'
                        if len(components) > 1 and components[1].lower() == 'desc':
                            direction = 'DESC'
                        
                        order_parts.append(f"{col_name_quoted} {direction}")
                if order_parts:
                    order_by_clause = f" ORDER BY {', '.join(order_parts)}"
                else:
                    logger.warning(f"No valid orderby columns found after sanitization: {orderby_param}. Ignoring orderby.")

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

            final_query = f'SELECT {columns_to_select} FROM {safe_entity_set_quoted}{where_clause}{order_by_clause}{limit_clause}{offset_clause};'
            logger.info(f"Final SQL (GET) query: {final_query}")
            logger.info(f"SQL (GET) Parameters: {sql_params}")

            cur.execute(final_query, sql_params)
            rows = cur.fetchall()

            column_names = [desc[0] for desc in cur.description]

            data = []
            for row in rows:
                row_dict = {}
                for i, col_value in enumerate(row):
                    if isinstance(col_value, (type(None))):
                        row_dict[column_names[i]] = None
                    elif hasattr(col_value, 'isoformat'):
                        row_dict[column_names[i]] = col_value.isoformat()
                    else:
                        row_dict[column_names[i]] = col_value
                data.append(row_dict)

            odata_response = {
                "@odata.context": f"/api/odata/$metadata#{safe_entity_set_raw}",
                "value": data
            }
            logger.info(f"Successfully retrieved {len(data)} records for {safe_entity_set_raw}.")
            return jsonify(odata_response)

        elif request.method == 'POST':
            # --- POST Logic (New Code) ---
            logger.info(f"Received POST request body: {request.json}")
            new_record_data = request.json

            if not new_record_data:
                return jsonify({"error": "No data provided in POST request body."}), 400

            columns = []
            values = []
            placeholders = []

            # Iterate through the received data to build the INSERT query
            # We assume the keys in new_record_data match your DB column names (with spaces if applicable)
            for key, value in new_record_data.items():
                # Important: Quote column names to handle spaces or specific casing in DB
                columns.append(f'"{key}"')
                values.append(value)
                placeholders.append('%s') # Use %s for parameterized query

            if not columns:
                return jsonify({"error": "No valid columns found in POST data."}), 400

            # Construct the INSERT query. Use RETURNING * to get all columns of the new row, including the generated 'id'.
            insert_query = f'INSERT INTO {safe_entity_set_quoted} ({", ".join(columns)}) VALUES ({", ".join(placeholders)}) RETURNING *;'
            
            logger.info(f"Final SQL (POST) query: {insert_query}")
            logger.info(f"SQL (POST) Parameters: {values}")

            cur.execute(insert_query, values)
            conn.commit() # Commit the transaction for POST/PUT/DELETE operations
            
            # Fetch the newly created record
            new_row = cur.fetchone()
            new_row_columns = [desc[0] for desc in cur.description] # Get column names for the returned row

            created_record = {}
            if new_row:
                for i, col_value in enumerate(new_row):
                    # Convert types similarly to GET
                    if isinstance(col_value, (type(None))):
                        created_record[new_row_columns[i]] = None
                    elif hasattr(col_value, 'isoformat'):
                        created_record[new_row_columns[i]] = col_value.isoformat()
                    else:
                        created_record[new_row_columns[i]] = col_value
            
            logger.info(f"Successfully created record with data: {created_record}")
            # Return 201 Created status with the newly created entity
            return jsonify(created_record), 201

    except ValueError as ve:
        logger.error(f"Client-side or configuration error: {ve}")
        return jsonify({"error": str(ve)}), 400
    except psycopg2.Error as pg_err:
        conn.rollback() # Rollback transaction on database error
        logger.exception(f"Database error during query execution: {pg_err}")
        return jsonify({"error": f"Database error: {pg_err.pgerror}", "sqlstate": pg_err.pgcode}), 500
    except Exception as e:
        logger.exception(f"An unexpected server error occurred: {e}")
        return jsonify({"error": f"An unexpected server error occurred: {e}"}), 500
    finally:
        # Ensure database connection and cursor are closed even if errors occur
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")


# For local development:
if __name__ == '__main__':
    if not os.getenv("NEON_DATABASE_URL"):
        logger.warning("NEON_DATABASE_URL environment variable is not set locally. "
                       "Please set it for local testing or it will fail.")
    app.run(debug=True, port=5000)
