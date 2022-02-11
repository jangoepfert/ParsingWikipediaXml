import xml.etree.ElementTree as ET


class ETParser:
    def __init__(self, file_obj, queue, shutdown_event):
        self._file_obj = file_obj
        self._queue = queue
        self._shutdown_event = shutdown_event
        self._page = None
        self._ns = None
        self._tags_stack = None
        self._title = None
        self._id = None
        self._inside_revision = False

    def parse(self):
        for event, element in ET.iterparse(self._file_obj, events=("start", "end")):
            tag_name = element.tag.rsplit("}", 1)[-1].strip()

            if event == "start":
                if tag_name == "page":
                    self._page = ""
                    self._tags_stack = []
                    self._title = ""
                    self._ns = 0
                    self._id = None
                    self._inside_revision = False
                elif tag_name == "revision":
                    self._inside_revision = True

                if self._page is not None:
                    self._tags_stack.append(tag_name)

            else:
                if self._page is not None:
                    # inside page tags

                    if element.text is not None:
                        if self._tags_stack[-1] == "text":
                            self._page += element.text
                        elif self._tags_stack[-1] == "title":
                            self._title = element.text
                        elif self._tags_stack[-1] == "ns":
                            self._ns = int(element.text)
                        elif self._tags_stack[-1] == "id" and not self._inside_revision:
                            self._id = int(element.text)

                    if self._tags_stack[-1] == "page":
                        if (
                            self._page is not None
                            and self._ns is not None
                            and self._ns == 0
                        ):
                            self._queue.put((self._id, self._title, self._page))
                        self._page = None
                        self._tags_stack = None
                    else:
                        del self._tags_stack[-1]

                element.clear()

        print("===> shutdown event is being set in et parser...")
        self._shutdown_event.set()
