__author__ = "Stefanos Amanuel"
__copyright__ = "Your Friends House"
__version__ = "0.0.1"

from flask import Flask, render_template, request, jsonify, session, send_file
import os
import csv
import time
import requests
import easypost
from PIL import Image
import uuid
from io import BytesIO
from flask_basicauth import BasicAuth
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))
app.config['UPLOAD_FOLDER'] = 'tmp'

# Basic Auth Configuration
app.config['BASIC_AUTH_USERNAME'] = os.environ.get('BASIC_AUTH_USERNAME')
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('BASIC_AUTH_PASSWORD')
app.config['BASIC_AUTH_FORCE'] = True
basic_auth = BasicAuth(app)

# Logging setup
LOG_FILE = 'logs/access.log'

def log_access():
    ip = request.remote_addr
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    user_agent = request.headers.get('User-Agent', 'Unknown')
    
    log_entry = f"{timestamp} - {ip} - {user_agent}\n"
    
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)

@app.before_request
def before_request():
    if basic_auth.authenticate():
        log_access()

def process_row(row, format_type, return_address):
    try:
        base_address = {
            'from_address': return_address,
            'options': {
                "label_format": "PNG",
                "label_size": "4x6"
            }
        }

        if format_type == 'mana_pool':
            return {
                **base_address,
                'to_address': {
                    "name": row.get('shipping_name', ''),
                    "street1": row.get('shipping_line1', ''),
                    "street2": row.get('shipping_line2', ''),
                    "city": row.get('shipping_city', ''),
                    "state": row.get('shipping_state', ''),
                    "zip": row.get('shipping_zip', ''),
                    "country": "US"
                },
                'parcel': {
                    "weight": float(row.get('Product Weight', 1.0)),
                    "predefined_package": "Letter" if float(row.get('Product Weight', 3.5)) < 3.5 else "Flat"
                },
                'value_of_goods': row.get('total', '0')
            }
        else:
            return {
                **base_address,
                'to_address': {
                    "name": f"{row.get('FirstName', '')} {row.get('LastName', '')}".strip(),
                    "street1": row.get('Address1', ''),
                    "street2": row.get('Address2', ''),
                    "city": row.get('City', ''),
                    "state": row.get('State', row.get('shipping_state', '')),
                    "zip": row.get('PostalCode', row.get('shipping_zip', '')),
                    "country": "US"
                },
                'parcel': {
                    "weight": float(row.get('Product Weight', 1.0)),
                    "predefined_package": "Letter" if float(row.get('Product Weight', 3.5)) < 3.5 else "Flat"
                },
                'value_of_goods': row.get('Value Of Products', row.get('total', '0'))
            }
    except Exception as e:
        raise ValueError(f"Error processing row: {str(e)}. Row data: {row}")

@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    csv_data = []
    reader = csv.DictReader(file.stream.read().decode('utf-8').splitlines())
    for row in reader:
        csv_data.append(dict(row))

    file_id = str(uuid.uuid4())
    session[file_id] = {
        'data': csv_data,
        'env': request.form.get('environment', 'development'),
        'prod_key': request.form.get('prod_key'),
        'dev_key': request.form.get('dev_key'),
        'return_address': {
            'name': request.form.get('return_name'),
            'street1': request.form.get('return_street1'),
            'street2': request.form.get('return_street2'),
            'city': request.form.get('return_city'),
            'state': request.form.get('return_state'),
            'zip': request.form.get('return_zip'),
            'country': 'US'
        }
    }
    
    return jsonify({'file_id': file_id, 'data': csv_data})

@app.route('/review/<file_id>', methods=['GET'])
def review(file_id):
    stored_data = session.get(file_id, {})
    return render_template('review.html',
        file_id=file_id,
        rows=stored_data.get('data', []),
        return_address=stored_data.get('return_address', {})
    )

@app.route('/generate/<file_id>', methods=['POST'])
def generate(file_id):
    stored_data = session.get(file_id)
    if not stored_data:
        return jsonify({'error': 'Invalid session'}), 400

    try:
        edited_data = request.get_json()
        api_key = stored_data['prod_key'] if stored_data['env'] == 'production' else stored_data['dev_key']
        return_address = edited_data['return_address']
        
        label_images = []
        client = easypost.EasyPostClient(api_key)

        for i, row in enumerate(edited_data['form_data']):
            try:
                # Determine format type based on available fields
                format_type = 'mana_pool' if 'shipping_name' in row else 'tcgplayer'
                address_info = process_row(row, format_type, return_address)

                # Validate required fields
                required_fields = ['name', 'street1', 'city', 'state', 'zip']
                to_address = address_info['to_address']
                missing_fields = [field for field in required_fields if not to_address.get(field)]
                
                if missing_fields:
                    raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

                # Retry logic
                for attempt in range(5):
                    try:
                        shipment = client.shipment.create(**address_info)
                        bought_shipment = client.shipment.buy(shipment.id, rate=shipment.lowest_rate())

                        if float(address_info['value_of_goods']) > 15:
                            client.shipment.insure(bought_shipment.id, amount=address_info['value_of_goods'])

                        img = Image.open(requests.get(bought_shipment.postage_label.label_url, stream=True).raw)
                        label_images.append(img)
                        time.sleep(1)
                        break
                    except Exception as e:
                        if 'To Many Requests' in str(e):
                            time.sleep(2 ** attempt)
                        else:
                            raise

            except Exception as e:
                print(f"Error processing row {i}: {str(e)}")
                continue

        if not label_images:
            return jsonify({'error': 'No labels were generated. Check your data and try again.'}), 400

        # Create PDF
        pdf_io = BytesIO()
        label_images[0].save(pdf_io, save_all=True, append_images=label_images[1:], format='PDF')
        pdf_io.seek(0)
        
        return send_file(pdf_io, mimetype='application/pdf', download_name='labels.pdf')

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    app.run(host='0.0.0.0')