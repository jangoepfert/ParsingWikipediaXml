import os
import re
import json
import time
from datetime import timedelta
from multiprocessing import Manager, Process, cpu_count, Event
from threading import Thread
from bz2 import BZ2File
from argparse import ArgumentParser
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


def process_entries(read_queue_, output_queue_, shutdown_event):
    redirect_pattern = re.compile("#REDIRECT", re.IGNORECASE)

    while not (shutdown_event.is_set() and read_queue_.empty()):
        if not read_queue_.empty():
            id, title, page = read_queue_.get()
            if not bool(redirect_pattern.match(page)):
                row = json.dumps({"id": id, "title": title, "text": page})
                output_queue_.put(row)

    print("==> exiting from process page while loop")


def print_info(report_queue, process_shutdown_event):

    start = time.time()
    print("starting with reading, processing and writting ...")
    number_of_pages = 0

    while not (process_shutdown_event.is_set() and report_queue.empty()):
        if not report_queue.empty():
            number_of_pages += report_queue.get()
            if number_of_pages % 10000 == 0 and number_of_pages != 0:
                now = time.time()
                delta = now - start
                velocity = number_of_pages / delta
                remainig_time = (20_620_000 - number_of_pages) / velocity
                delta = str(timedelta(seconds=delta))
                remainig_time = str(timedelta(seconds=remainig_time))
                print(
                    f"{number_of_pages} pages are written to file in {delta} seconds, expected remaining={remainig_time}"
                )


def write_to_file(
    output_file,
    output_queue,
    report_queue,
    write_shutdown_event,
    process_shutdown_event,
):

    # Write a JSON row per wikipedia page to the output file
    while not (write_shutdown_event.is_set() and output_queue.empty()):
        if not output_queue.empty():
            row = output_queue.get()
            output_file.write(row + "\n")
            report_queue.put(1)

    print("==> exiting write while loop and closing the file")
    output_file.close()
    process_shutdown_event.set()


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument(
        "--dump_path",
        default="./../enwiki-latest-pages-articles-multistream.xml.bz2",
        help="""Path to local Wikipedia XML dump (e.g., 
        'enwiki-latest-pages-articles-multistream.xml.bz2')""",
    )
    parser.add_argument(
        "--outfile",
        default="./parsed_wikipedia_dump.json",
        help="""Path of output file (e.g., './parsed_wikipedia_dump.json').
        If nbr_outfiles is greater than one, a number will be added to the 
        filename (that is, parsed_wikipedia_dump_{i}.json).""",
    )
    parser.add_argument(
        "--nbr_outfiles",
        default=1,
        help="""Specify the number of files to split the output into (e.g., 2)""",
    )
    args = parser.parse_args()

    queue_manager = Manager()
    read_queue = queue_manager.Queue(maxsize=2000)
    output_queue = queue_manager.Queue(maxsize=2000)
    report_queue = queue_manager.Queue(maxsize=1000)

    process_shutdown_event = Event()
    write_shutdown_event = Event()

    processes = []
    num_workers = max(1, cpu_count() - 1)
    for p in range(num_workers):
        p = Process(
            target=process_entries,
            args=(read_queue, output_queue, process_shutdown_event),
        )
        p.start()
        processes.append(p)

    # Open output files
    nbr_outfiles = int(args.nbr_outfiles)
    if nbr_outfiles < 1:
        nbr_outfiles = 1
        print(
            "Set the number of outfiles to 1, since \
            at least a single ouput file is required."
        )

    if nbr_outfiles == 1:
        output_files = [open(args.outfile, "w", encoding="utf-8")]
    else:
        base, extension = os.path.splitext(args.outfile)
        output_files = [
            open(base + "_" + str(nbr) + extension, "w", encoding="utf-8")
            for nbr in range(0, nbr_outfiles)
        ]

    output_threads = [
        Thread(
            target=write_to_file,
            args=(
                output_file,
                output_queue,
                report_queue,
                write_shutdown_event,
                process_shutdown_event,
            ),
        )
        for output_file in output_files
    ]

    print_info_thread = Thread(
        target=print_info, args=(report_queue, process_shutdown_event)
    )

    for output_thread in output_threads:
        output_thread.start()

    print_info_thread.start()

    wiki_file_obj = BZ2File(args.dump_path)

    # Parse XML dump
    et_wiki_parser = ETParser(wiki_file_obj, read_queue, write_shutdown_event)
    et_wiki_parser.parse()

    for thread in output_threads:
        thread.join()

    for p in processes:
        p.join()

    print_info_thread.join()
