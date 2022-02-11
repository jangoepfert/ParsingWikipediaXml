import re
import json


def process_entries(read_queue_, output_queue_, shutdown_event):
    redirect_pattern = re.compile("#REDIRECT", re.IGNORECASE)

    while not (shutdown_event.is_set() and read_queue_.empty()):
        if not read_queue_.empty():
            id, title, page = read_queue_.get()
            if not bool(redirect_pattern.match(page)):
                row = json.dumps({"id": id, "title": title, "text": page})
                output_queue_.put(row)

    print("==> exiting from process page while loop")
