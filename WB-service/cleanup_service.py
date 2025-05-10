import threading
import time
from datetime import datetime, timedelta
from database import db
from models import Subscription

DELETE_INTERVAL = 60 * 60
EXPIRY_TIME = timedelta(hours=72)

def cleanup_expired_subscriptions(app):
    def run():
        with app.app_context():
            while True:
                threshold_time = datetime.now() - EXPIRY_TIME
                expired = Subscription.query.filter(Subscription.created_at < threshold_time).all()
                
                if expired:
                    print(f"Deleting {len(expired)} expired subscriptions...")
                    for sub in expired:
                        db.session.delete(sub)
                    db.session.commit()
                else:
                    print("No expired subscriptions found.")
                
                time.sleep(DELETE_INTERVAL)
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
