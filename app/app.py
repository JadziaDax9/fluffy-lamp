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

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'tmp'
basic_auth = BasicAuth(app)



app.config['BASIC_AUTH_USERNAME'] = os.getenv('ADMIN_USER', 'admin')
app.config['BASIC_AUTH_PASSWORD'] = os.getenv('ADMIN_PASS', 'secret')
app.config['BASIC_AUTH_FORCE'] = True  # Protect all routes


# Helper functions
def process_row(row, format_type):
    base_address = {
        'from_address': {
            "name": "Your Friends House",
            "street1": "2776 S Arlington Mill Dr",
            "street2": "PMB 516",
            "city": "Arlington",
            "state": "VA",
            "zip": "22206",
            "country": "US"
        },
        'options': {
            "label_format": "PNG",
            "label_size": "4x6"
        }
    }

    if format_type == 'mana_pool':
        return {
            **base_address,
            'to_address': {
                "name": row['shipping_name'],
                "street1": row['shipping_line1'],
                "street2": row['shipping_line2'],
                "city": row['shipping_city'],
                "state": row['shipping_state'],
                "zip": row['shipping_zip'],
                "country": "US"
            },
            'parcel': {
                "weight": float(row.get('Product Weight', 1.0)),
                "predefined_package": "Letter" if float(row.get('Product Weight', 3.5)) < 3.5 else "Flat"
            },
            'value_of_goods': row['total']
        }
    else:  # TCGplayer
        return {
            **base_address,
            'to_address': {
                "name": f"{row['FirstName']} {row['LastName']}",
                "street1": row['Address1'],
                "street2": row['Address2'],
                "city": row['City'],
                "state": row['State'],
                "zip": row['PostalCode'],
                "country": "US"
            },
            'parcel': {
                "weight": float(row.get('Product Weight', 1.0)),
                "predefined_package": "Letter" if float(row.get('Product Weight', 3.5)) < 3.5 else "Flat"
            },
            'value_of_goods': row['Value Of Products']
        }

# Routes
@app.route('/', methods=['GET'])
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Read and store CSV data
    csv_data = []
    reader = csv.DictReader(file.stream.read().decode('utf-8').splitlines())
    for row in reader:
        csv_data.append(dict(row))
    
    # Store in session with unique ID
    file_id = str(uuid.uuid4())
    session[file_id] = {
        'data': csv_data,
        'env': request.form.get('environment', 'development'),
        'prod_key': request.form.get('prod_key'),
        'dev_key': request.form.get('dev_key')
    }
    
    return jsonify({'file_id': file_id, 'data': csv_data})

@app.route('/review/<file_id>', methods=['GET'])
def review(file_id):
    data = session.get(file_id, {}).get('data', [])
    return render_template('review.html', file_id=file_id, rows=data)

@app.route('/generate/<file_id>', methods=['POST'])
def generate(file_id):
    stored_data = session.get(file_id)
    if not stored_data:
        return jsonify({'error': 'Invalid session'}), 400

    # Get edited data from form
    edited_data = request.get_json()
    
    # Process labels
    try:
        label_images = []
        api_key = stored_data['prod_key'] if stored_data['env'] == 'production' else stored_data['dev_key']
        client = easypost.EasyPostClient(api_key)

        for i, row in enumerate(edited_data):
            try:
                format_type = 'mana_pool' if 'shipping_name' in row else 'tcgplayer'
                address_info = process_row(row, format_type)

                # Retry logic (same as original)
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

        # Create PDF
        pdf_io = BytesIO()
        label_images[0].save(pdf_io, save_all=True, append_images=label_images[1:], format='PDF')
        pdf_io.seek(0)
        
        return send_file(pdf_io, mimetype='application/pdf', download_name='labels.pdf')

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)