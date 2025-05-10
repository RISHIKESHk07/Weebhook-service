import threading
import requests
from datetime import datetime
from models import db, WebhookDelivery, Subscription, WebhookEvent
from flask import Flask
from queue import Queue
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import time

# Maximum number of retry attempts
MAX_TRIES = 5

scheduler = BackgroundScheduler()

# Create the queue (simple in-memory queue)
event_queue = Queue()

def retry_job(subscription_id, event_id):
    event_queue.put({'subscription_id': subscription_id, 'event_id': event_id})

def process_delivery_job(subscription_id, event_id):
    """
    Process the delivery job. This function is used in the background worker.
    """
    print(subscription_id,event_id)
    # Query event and subscription
    event = WebhookEvent.query.filter_by(id = event_id).first()
    subscription = Subscription.query.filter_by(id = subscription_id).first()


    # Get the previous delivery count (attempts)
    delivery_count = WebhookDelivery.query.filter_by(
        subscription_id=subscription.id,
        event_id=event.id
    ).count()

    try:
        # Simulate sending a POST request to the subscription target URL
        response = requests.post(
            "http://localhost:5000/webhook",
            json=event.payload,
            timeout=10
        )

        # Log delivery success or failure
        log = WebhookDelivery(
            subscription_id=subscription.id,
            event_id=event.id,
            attempt_count=delivery_count + 1,
            status="success" if response.ok else "failed",
            http_code=response.status_code,
            Error_details=None if response.ok else response.text
        )
        db.session.add(log)
        db.session.commit()

    except Exception as e:
        db.session.rollback()

        # Retry delivery if it's below the max retries
        if delivery_count < MAX_TRIES:
            # Log retry (pending)
            db.session.add(WebhookDelivery(
                subscription_id=subscription.id,
                event_id=event.id,
                attempt_count=delivery_count + 1,
                status="pending",
                http_code=None,
                Error_details=str(e)
            ))
            db.session.commit()

            # Requeue job for retry
            scheduler.add_job(retry_job, 'date', run_date=datetime.now() + timedelta(minutes=1), args=[subscription.id, event.id])

        else:
            # Log failure after max retries
            db.session.add(WebhookDelivery(
                subscription_id=subscription.id,
                event_id=event.id,
                attempt_count=delivery_count + 1,
                status="failed",
                http_code=None,
                Error_details=str(e)
            ))
            db.session.commit()

def queue_worker(app):
    """
    This function runs in a separate thread and continuously processes jobs from the queue.
    """
    with app.app_context():

       while True:
           # Block until an item is available in the queue
           job = event_queue.get()
           if job is None:
               break  # Exit if None is received (stop signal)
           
           # Extract subscription_id and event_id from the job
           print(type(job))
           subscription_id, event_id = job['subscription_id'],job['event_id']
        #    print(job,job['subscription_id'],job['event_id'])
           process_delivery_job(subscription_id,event_id)
           event_queue.task_done()  # Mark the task as done