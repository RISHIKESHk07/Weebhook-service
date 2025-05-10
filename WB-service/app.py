from flask import Flask, request, jsonify
from database import db
from models import Subscription, WebhookEvent, WebhookDelivery
from datetime import datetime
import requests
from flask_migrate import Migrate
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://webhook_user:supersecret@localhost:5434/webhooks_db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()
    
migrate = Migrate(app, db)


# Create a subscription for a company
@app.route('/subscriptions', methods=['POST'])
def create_subscription():
    data = request.json
    sub = Subscription(
        target_url=data['target_url'],
        event_type=data['event_type'],
        secret_key=data.get('secret_key')
    )
    db.session.add(sub)
    db.session.commit()
    return jsonify({'id': sub.id}), 201

# Trigger an event of weebhook over here 
@app.route('/events', methods=['POST'])
def create_event():
    data = request.json
    event = WebhookEvent(event_type=data['event_type'], payload=data['payload'])
    db.session.add(event)
    db.session.commit()

    # Queue deliveries , using a lib for this redis
    # subscriptions = Subscription.query.filter_by(event_type=event.event_type, is_active=True).all()
    #
    #
    # db.session.commit()

    return jsonify({'event_id': event.id}), 201

# View all current subscriptions
@app.route('/subscriptions', methods=['GET'])
def list_subscriptions():
    subs = Subscription.query.all()
    return jsonify([{'id': s.id, 'url': s.target_url, 'event': s.event_type} for s in subs])

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)