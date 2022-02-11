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
