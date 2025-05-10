from flask import Flask, request, jsonify
from database import db
from models import Subscription, WebhookEvent, WebhookDelivery
from datetime import datetime
import requests
from flask_migrate import Migrate
import os,threading
import hmac,hashlib
from queue_worker import event_queue,queue_worker,scheduler
from cleanup_service import cleanup_expired_subscriptions
import json
import redis
import pickle

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


#DB intiation & Migration
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://webhook_user:supersecret@localhost:5432/webhooks_db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()
    
migrate = Migrate(app, db)

# caching helper functions 
def cache_subscription(sub):
    key = f"subscription:{sub.id}"
    redis_client.set(key, pickle.dumps(sub))

def get_cached_subscription(subscription_id):
    key = f"subscription:{subscription_id}"
    cached = redis_client.get(key)
    if cached:
        return pickle.loads(cached)
    return None

def remove_cached_subscription(subscription_id):
    key = f"subscription:{subscription_id}"
    redis_client.delete(key)


def signature_validity(payload , recieved_signature ,sub_id_key):
    key_bytes = sub_id_key.encode('utf-8')
    payload_bytes = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode('utf-8')
    computed_signature = hmac.new(key_bytes,payload_bytes,hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed_signature,recieved_signature)


def start_background_worker(app):
    """
    This function starts the background worker in a new thread.
    """
    worker_thread = threading.Thread(target=queue_worker, args=(app,) , daemon=True)
    worker_thread.start()
    
if __name__ == "__main__":
    scheduler.start()
    cleanup_expired_subscriptions(app)
    start_background_worker(app)
    @app.route('/ingest/<subscription_id>',methods=['POST'])
    def intiate_webhook(subscription_id):
        payload = request.get_json()
        event_type = payload.get("event")
        sig = request.headers.get('x-signature')
        sub = get_cached_subscription(subscription_id)

        if not sub :
            sub = Subscription.query.filter_by(id=subscription_id).first()
            if not sub :
                return jsonify({'message':'Subscription not found'}) , 404
            cache_subscription(sub)

        if not signature_validity(payload,sig,sub.secret_key):
            jsonify({"message":"Not valid"})
        
        #Add a event for the this current ingest
        event = WebhookEvent(event_type=event_type,payload=payload)
        db.session.add(event)
        db.session.commit()
        
        #Add it to the queue for processing
        event_queue.put({
            "event_id": event.id,
            "subscription_id": subscription_id
        })
        
        response_message = {"message": "Request accepted for processing"}
        return jsonify(response_message), 200
    # View all current subscriptions
    @app.route('/subscriptions', methods=['GET'])
    def list_subscriptions():
        subs = Subscription.query.all()
        return jsonify([{'id': s.id, 'url': s.target_url, 'event': s.event_type} for s in subs])
    
    @app.route('/add_subscription', methods=['POST'])
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
    @app.route('/webhook',methods=['POST'])
    def test_webhook():
        print("Reached the target url")
        return jsonify({"messgae":"Post reached to target url"})
    @app.route('/subscription_deliveries/<subscription_id>', methods=['GET'])
    def get_recent_deliveries(subscription_id):
        deliveries = WebhookDelivery.query.filter_by(subscription_id=subscription_id).order_by(WebhookDelivery.created_at.desc()).limit(20).all()
        if not deliveries:
            return jsonify({"message": "No deliveries found for this subscription"}), 404
    
        return jsonify([
            {
                "event_id": d.event_id,
                "attempt": d.attempt_count,
                "status": d.status,
                "http_code": d.http_code,
                "Error_details": d.Error_details,
                "timestamp": d.created_at.isoformat()
            } for d in deliveries
        ])
    @app.route('/delivery_status/<event_id>', methods=['GET'])
    def get_delivery_status(event_id):
        deliveries = WebhookDelivery.query.filter_by(event_id=event_id).order_by(WebhookDelivery.attempt_count).all()
        
        if not deliveries:
            return jsonify({"message": "No delivery attempts found for this event"}), 404
    
        return jsonify([
            {
                "attempt": d.attempt_count,
                "status": d.status,
                "http_code": d.http_code,
                "Error_details": d.Error_details,
                "timestamp": d.created_at.isoformat()
            } for d in deliveries
        ])
    
    app.run(host='0.0.0.0', port=8000, debug=True)
    # Update a subscription
@app.route('/update_subscription/<subscription_id>', methods=['PUT'])
def update_subscription(subscription_id):
    sub = Subscription.query.filter_by(id=subscription_id).first()
    if not sub:
        return jsonify({"message": "Subscription not found"}), 404

    data = request.json
    sub.target_url = data.get('target_url', sub.target_url)
    sub.event_type = data.get('event_type', sub.event_type)
    sub.secret_key = data.get('secret_key', sub.secret_key)
    sub.is_active = data.get('is_active', sub.is_active)

    db.session.commit()
    cache_subscription(sub)
    return jsonify({"message": "Subscription updated successfully"}), 200


# Delete a subscription
@app.route('/delete_subscription/<subscription_id>', methods=['DELETE'])
def delete_subscription(subscription_id):
    sub = Subscription.query.filter_by(id=subscription_id).first()
    if not sub:
        return jsonify({"message": "Subscription not found"}), 404

    db.session.delete(sub)
    db.session.commit()
    remove_cached_subscription(subscription_id) 
    return jsonify({"message": "Subscription deleted successfully"}), 200
