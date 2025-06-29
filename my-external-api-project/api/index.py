# api/index.py
import os
import json
import psycopg2
from flask import Flask, request, jsonify
from urllib.parse import urlparse, parse_qs

app = Flask(__name__)

# IMPORTANT: Store this securely in Vercel environment variables!
# The NEON_DATABASE_URL should look like:
# postgresql://user:password@ep-host.region.aws.neon.tech/dbname?sslmode=require
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")

def get_db_connection():
    if not NEON_DATABASE_URL:
        raise ValueError("NEON_DATABASE_URL environment variable is not set.")
    return psycopg2.connect(NEON_DATABASE_URL)

@app.route('/api/odata/<path:entity_set>', methods=['GET'])
def odata_endpoint(entity_set):
    # This is a very basic OData implementation.
    # A full OData v4 server handles much more complexity (e.g., $expand, $batch, metadata)
    # For a simple Salesforce External Object, GET requests for a single entity set are common.

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Basic OData query parameters
        top = request.args.get('$top')
        skip = request.args.get('$skip')
        filter_param = request.args.get('$filter')
        orderby_param = request.args.get('$orderby')
        select_param = request.args.get('$select')

        # === IMPORTANT: SQL Injection Prevention ===
        # Directly using user input in SQL is dangerous.
        # This example uses f-strings for simplicity, but for production:
        # 1. Use parameterized queries for values (psycopg2 handles this with %s).
        # 2. For column names and table names, you MUST sanitize or use a whitelist.
        #    Example: if entity_set not in ['products', 'customers']: raise InvalidTableError
        #    Example: if column_name not in ['id', 'name', 'price']: raise InvalidColumnError

        # --- Build SELECT clause ---
        if select_param:
            # Basic sanitization for select_param - split by comma and strip, then join
            columns_to_select = ", ".join([col.strip() for col in select_param.split(',') if col.strip()])
        else:
            columns_to_select = "*"

        query = f"SELECT {columns_to_select} FROM {entity_set}"
        params = []
        param_index = 1 # For psycopg2 parameterized queries

        # --- Build WHERE clause from $filter ---
        if filter_param:
            # WARNING: This is an EXTREMELY simplified and INSECURE filter parser.
            # A production OData service would need a robust parser to translate
            # OData filter syntax (e.g., 'Name eq "Alice"') into secure SQL WHERE clauses.
            # Example for a simple 'PropertyName eq Value' filter:
            # if ' eq ' in filter_param:
            #     prop, val = filter_param.split(' eq ', 1)
            #     # Further parsing to remove quotes from val if string
            #     query += f" WHERE {prop} = %s"
            #     params.append(val.strip('"\''))
            # For this example, we'll just allow direct passthrough if needed, but DO NOT USE IN PROD.
            print(f"Warning: Using unsanitized $filter '{filter_param}'. Implement proper OData parsing and SQL sanitization.")
            query += f" WHERE {filter_param}"


        # --- Build ORDER BY clause ---
        if orderby_param:
            # Simple sanitization for orderby_param - split by comma and strip
            # Still potentially vulnerable if column names can be injected.
            clean_orderby = ", ".join([col.strip() for col in orderby_param.split(',') if col.strip()])
            query += f" ORDER BY {clean_orderby}"


        # --- Build LIMIT and OFFSET clauses from $top and $skip ---
        if top:
            query += f" LIMIT %s"
            params.append(int(top))
        if skip:
            query += f" OFFSET %s"
            params.append(int(skip))

        # Execute the query
        cur.execute(query, params)
        rows = cur.fetchall()

        # Get column names for building the OData response
        column_names = [desc[0] for desc in cur.description]

        # Format data as list of dictionaries for OData JSON
        data = []
        for row in rows:
            data.append(dict(zip(column_names, row)))

        # Construct OData v4 JSON response
        odata_response = {
            "@odata.context": f"/api/odata/$metadata#{entity_set}", # Or a more precise metadata URL
            "value": data
        }

        cur.close()
        conn.close()

        return jsonify(odata_response)

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except psycopg2.Error as pg_err:
        return jsonify({"error": f"Database error: {pg_err.pgerror}"}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500

# Vercel requires a specific entry point for Python functions
# If you name your file `api/index.py`, Flask's app object is often directly callable.
# Or, if you need a specific handler for Vercel:
# from your_app import app as application # if 'app' is defined in another file

# For local development:
if __name__ == '__main__':
    # Set a dummy environment variable for local testing if you don't have it set already
    # os.environ["NEON_DATABASE_URL"] = "postgresql://user:password@ep-host.region.aws.neon.tech/dbname?sslmode=require"
    app.run(debug=True)