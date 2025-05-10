from datetime import datetime
from database import db
import uuid

def generate_uuid():
    return str(uuid.uuid4())

class Subscription(db.Model):
    __tablename__ = 'subscriptions'
    id = db.Column(db.String, primary_key=True, default=generate_uuid)
    target_url = db.Column(db.String, nullable=False)
    secret_key = db.Column(db.String)
    event_type = db.Column(db.String, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.now)

class WebhookEvent(db.Model):
    __tablename__ = 'webhook_events'
    id = db.Column(db.String, primary_key=True, default=generate_uuid)
    event_type = db.Column(db.String, nullable=False)
    payload = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

class WebhookDelivery(db.Model):
    __tablename__ = 'webhook_deliveries'
    id = db.Column(db.String, primary_key=True, default=generate_uuid)
    subscription_id = db.Column(db.String, db.ForeignKey('subscriptions.id'), nullable=False)
    event_id = db.Column(db.String, db.ForeignKey('webhook_events.id'), nullable=False)
    attempt_count = db.Column(db.Integer, default=0)
    status = db.Column(db.String, default='pending')  # 'pending', 'success', 'failed'
    http_code=db.Column(db.Integer,default=000)
    Error_details=db.Column(db.String)
    created_at = db.Column(db.DateTime, default=datetime.now)
