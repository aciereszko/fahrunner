import datetime
import getopt
import json
import logging
import signal
import sys
import telnetlib
import time


def print_with_time(stuff):
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{time_now} : {stuff}")


def isBigger(time1, time2):
    try:
        time_a = datetime.datetime.strptime(time1, "%M mins %S secs")
    except ValueError:
        try:
            time_a = datetime.datetime.strptime(time1, "%S secs")
        except ValueError:
            try:
                time_a = datetime.datetime.strptime(time1, "%H hours %M mins")
                return True
            except ValueError:
                return False

    time_b = datetime.datetime.strptime(time2, "%M:%S")
    return time_a > time_b


def get_slots_to_reinit(host, queue_info):
    slots = []

    queue_info_string = queue_info.decode("utf-8")

    logging.debug("queue_info_string: ***" + queue_info_string + "***")

    beg_json_index = queue_info_string.find("[")
    end_json_index = queue_info_string.rfind("]")

    if beg_json_index == -1 or end_json_index == -1:
        logging.error("Malformed json, no brackets, array of slots: " + queue_info_string)
        return slots

    logging.debug(f"beg_json: {beg_json_index} end_json: {end_json_index}")
    logging.debug(f"json: {queue_info_string[beg_json_index: end_json_index + 1]}")

    fah_json = json.loads(queue_info_string[beg_json_index: end_json_index + 1])

    for slot in fah_json:

        slot_id = slot['slot']

        slot_state = slot['state']
        if slot_state == "DOWNLOAD":
            if "nextattempt" in slot:
                next_attempt = slot['nextattempt']
                logging.debug(f"nextattempt: {next_attempt}")

                attempts = ""
                if "attempts" in slot:
                    attempts = slot['attempts']

                print_with_time(f"Host {host} has slot {slot_id} idle! Next attempt {next_attempt}. Attempts {attempts}")

                if isBigger(next_attempt, "2:00"):
                    slots.append(slot_id)

    return slots


def reinit_slot(tn, slot):
    print_with_time(f"Slot = {slot} doing nothing! Taking action.")
    tn.write(b"pause " + slot.encode() + b"\n")
    print_with_time(f"Pausing...")
    time.sleep(10)
    tn.write(b"unpause " + slot.encode() + b"\n")
    print_with_time(f"Unpausing...")


def main(argv):
    debug = False
    try:
        opts, args = getopt.getopt(argv, "hd")
    except getopt.GetoptError:
        print('fahrunner.py -h')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('fahrunner.py -d -h')
            sys.exit()
        elif opt == '-d':
            debug = True
            logging.shutdown()

    if debug:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

    # change this array to reflect your IP folding hosts situation, remember to allow traffic from computer running this script
    hosts = ["192.168.3.102", "192.168.3.103"]
    # port for remote access (telnet) on folding machines
    port = "36330"

    def handler(signal_received, frame):
        # Handle any cleanup here
        print_with_time('SIGINT or CTRL-C detected. Exiting gracefully')
        tn.write(b"exit\n")
        exit(0)

    signal.signal(signal.SIGINT, handler)

    while True:
        for host in hosts:
            try:
                tn = telnetlib.Telnet(host, port)
                tn.read_until(b"> ")
                tn.write(b"queue-info\n")

                for slot_to_reinit in get_slots_to_reinit(host, tn.read_until(b"> ")):
                    reinit_slot(tn, slot_to_reinit)

                tn.write(b"exit\n")
            except TimeoutError as te:
                print_with_time(f"Timeout when trying to establish connection to {host} {te}")
                pass
            except OSError as ose:
                print_with_time(f"Timeout when trying to establish connection to {host} {ose}")
                pass
        time.sleep(60)


if __name__ == "__main__":
    main(sys.argv[1:])
