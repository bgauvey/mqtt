# Tag Change Script or Message Handler
def onMessageReceived(topic, payload):
    import json
    data = json.loads(payload)
    
    order_number = data['orderNumber']
    status = data['statusName']
    
    # Update tags, trigger alerts, etc.
    system.tag.writeBlocking(
        [f"[default]jobs/{order_number}/Status"],
        [status]
    )