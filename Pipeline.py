import os
import sys
from multiprocessing import Manager, Process, cpu_count, Event
from threading import Thread
from bz2 import BZ2File
import xml.sax
from saxHandler import CustomContentHandler
from etParser import ETParser
from ProcessRawPages import process_entries
from writeToFile import write_to_file
from reportProgress import print_info
from argparse import ArgumentParser

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
    if args.nbr_outfiles < 1:
        args.nbr_outfiles = 1
        print(
            "Set the number of outfiles to 1, since \
            at least a single ouput file is required."
        )

    if args.nbr_outfiles == 1:
        output_files = [open(args.outfile, "w", encoding="utf-8")]
    else:
        base, extension = os.path.splitext(args.outfile)
        output_files = [
            open(base + "_" + str(nbr) + extension, "w", encoding="utf-8")
            for nbr in range(0, int(args.nbr_outfiles))
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

    # parsing with xml.sax
    # handler = CustomContentHandler(read_queue, write_shutdown_event)
    # xml.sax.parse(wiki_file_obj, handler)

    # parsing with xml.etree.ElementTree
    et_wiki_parser = ETParser(wiki_file_obj, read_queue, write_shutdown_event)
    et_wiki_parser.parse()

    for thread in output_threads:
        thread.join()

    for p in processes:
        p.join()

    print_info_thread.join()
