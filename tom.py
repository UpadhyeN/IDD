from pyModbusTCP.client import ModbusClient
from pyModbusTCP.utils import get_bits_from_int
from pyModbusTCP.utils import set_bit
from pyModbusTCP.utils import reset_bit
from pyModbusTCP.utils import test_bit
import paho.mqtt.client as mqtt
from time import sleep

from multiprocessing import BoundedSemaphore
from types import FunctionType

def methods(cls):
    return [x for x, y in cls.__dict__.items() if type(y) == FunctionType]

def power_minus(x):
    return

class TransportOutputModule:
    # Konstanten
    DIGITAL_INPUT_STARTING_ADDRESS = 8001
    DIGITAL_OUTPUT_STARTING_ADDRESS = 8018

    INDEX_CONVEYORS = ['A', 'B', 'D', 'E', 'G', 'H', 'I', 'K', 'L', 'N', 'O', 'P', 'T', 'U', 'V', 'W']
    INDEX_SWITCHES = ['C', 'F', 'J', 'M', 'Q', 'R', 'S', 'X']
    # Maps the identifiers of the conveyors, switches and separators to the digital inputs and outputs to which they are connected
    # Derived from tables 2.1, 2.2, 2.3 and 2.4 from chapter 2.1.3 of the hardware documentation
    INDEX = {
        # Treadmills Idize: [Bit Forward/Sensor Start, Bit Reverse/Sensor End]
        'A': [0, 1],
        'B': [2, 3],
        'D': [8, 9],
        'E': [10, 11],
        'G': [16, 17],
        'H': [18, 19],
        'I': [20, 21],
        'K': [26, 27],
        'L': [28, 29],
        'N': [34, 35],
        'O': [36, 37],
        'P': [38, 39],
        'T': [52, 53],
        'U': [54, 55],
        'V': [56, 57],
        'W': [58, 59],

        # Switch indices : [Reference run/Pos. Reached, pos.1/moving, pos.2/wstk in switch, pos.3/reference pos]
        'C': [4, 5, 6, 7],
        'F': [12, 13, 14, 15],
        'J': [22, 23, 24, 25],
        'M': [30, 31, 32, 33],
        'Q': [40, 41, 42, 43],
        'R': [44, 45, 46, 47],
        'S': [48, 49, 50, 51],
        'X': [60, 61, 62, 63],

        # Separator indices : [set, set, workpiece behind separator, workpiece before separator]
        'V1': [64, 64, 65, 66],
        'V2': [65, 67, 68, 69],
        'V3': [66, 70, 71, 72]

    }

    def __init__(self, ip_addr, read_write_sem=BoundedSemaphore(value=1), mqtt_broker=None, mqtt_topic=None):
        """
                Constructor of the TranporOutputModule.

                :param ip_addr IP address of the Modbus node responsible for the processing station (string)
                :param read_write_sem Semaphore that can be passed if you don't want 2 modules to send read/write commands at the same time
                """
        try:
            # Creates a connection to the Modbus with the ip_addr
            self.client = ModbusClient(host=ip_addr, auto_open=True, auto_close=True)
        except ValueError:
            print("Error with host param")

        # Semaphore, which ensures that only one thread can access the Modbus inputs and outputs at the same time
        self.sem = BoundedSemaphore(value=1)

        self.read_write_sem = read_write_sem
        self.mqtt_broker = "192.168.200.161"
        self.mqtt_topic = "Transport_out"
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(self.mqtt_broker)
        self.mqtt_client.loop_start()

        # Speed of the treadmills 0 = 0V/0% | 30000 = 10V/100%
        # All speeds to 0% by default
        self.conveyor_speed = {
            'A': 0,
            'B': 0,
            'D': 0,
            'E': 0,
            'G': 0,
            'H': 0,
            'I': 0,
            'K': 0,
            'L': 0,
            'N': 0,
            'O': 0,
            'P': 0,
            'T': 0,
            'U': 0,
            'V': 0,
            'W': 0
        }

        # Set up MQTT client if MQTT broker and topic are specified

    def get_input_register(self, offset=0, amount=1):
        """
        Returns the input registers of the Modbus.

        :param offset Offset to DIGITAL_INPUT_STARTING_ADDRESS
        :param amount Amount of registers to be read
        :returns list of registers read (or nothing if read fails)
        :rtype list of int or none
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.read_holding_registers(reg_addr=self.DIGITAL_INPUT_STARTING_ADDRESS + offset,
                                                            reg_nb=amount)
                if self.mqtt_broker and self.mqtt_topic:
                    payload = ",".join(str(x) for x in result)
                    self.mqtt_client.publish("Transport_out/Input_register", payload)
            return result

    def get_output_register(self, offset=0, amount=1):
        """
        Returns the Modbus output register.

        :param offset Offset to DIGITAL_OUTPUT_STARTING_ADDRESS
        :param amount Amount of registers to be read
        :returns list of registers read (or nothing if read fails)
        :rtype list of int or none
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.read_holding_registers(reg_addr=self.DIGITAL_OUTPUT_STARTING_ADDRESS + offset,
                                                            reg_nb=amount)
                if self.mqtt_broker and self.mqtt_topic:
                    payload = ",".join(str(x) for x in result)
                    self.mqtt_client.publish("Transport_out/Output_register", payload)
            return result

    def set_output_register(self, register, offset=0):
        """
        Overwrites the Modbus output register.

        :param register List of int to write to register
        :param offset Offset to DIGITAL_OUTPUT_STARTING_ADDRESS
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.write_multiple_registers(self.DIGITAL_OUTPUT_STARTING_ADDRESS + offset, register)
                topic = "Transport_out/output_register_set"
                message = f"Offset: {offset}, Register: {register}"
                self.mqtt_client.publish(topic, message)

    def get_offset(self, bit_nr):
        """
        Calculates the offset for get_input_register()/get_output_register()/set_output_register() based on the passed bit_no.
        :param bit_nr Number of the bit for which the offset is to be calculated
        :returns offset of the bit
        :rtype int
        """
        # Offset 0 -> 16 - 31
        # Offset 1 ->  0 - 15
        # Offset 2 -> 48 - 63
        # Offset 3 -> 32 - 47
        # Offset 5 -> 64 - 79
        # (Theoretically 79, but 67 is the highest bit needed, so offset 4 isn't actually needed)
        # Offset order is due to little endian order
        if bit_nr >= 16 and bit_nr <= 31:
            return 0
        if bit_nr >= 0 and bit_nr <= 15:
            return 1
        if bit_nr >= 48 and bit_nr <= 63:
            return 2
        if bit_nr >= 32 and bit_nr <= 47:
            return 3
        if bit_nr >= 80 and bit_nr <= 95:
            return 4
        if bit_nr >= 64 and bit_nr <= 79:
            return 5

    def get_bit(self, index, nr):
        """
        Returns the bit number required to access the correct bit within a word.
        :param index Index of the module/device to be addressed (see hardware documentation chapter 2.1.3)
        :param nr Number of the function for which the bit is required (see commas in the INDEX map)
        :returns Number of the bit
        :rtype int
        """
        # Number is calculated modulo 16, since a new word begins every 16 bits, with which addressing begins again..
        # ..with 0
        return self.INDEX.get(index)[nr] % 16

    def conveyor_stop(self, conveyor_id):
        """
        Stops the conveyor specified by conveyor_id by clearing the forward and reverse bits.
        The analog outputs that control the speed of the treadmills are not changed.
        :param conveyor_id the index of the conveyor as character (see hardware documentation chapter 2.1.3)
        """
        with self.sem:
            # Necessary offset is calculated to address the correct word
            # Since the bit for forwards/backwards is always in the same offset on the treadmills, it is sufficient to..
            # ..determine the offset from one of the two
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)

            reg = self.get_output_register(offset)
            # Clear forward and reverse bits to make the tape stop
            reg[0] = reset_bit(reg[0], bit_forward)
            reg[0] = reset_bit(reg[0], bit_backward)

            # Publish MQTT message with the conveyor_id and state change
            topic = "Transport_out/conveyor/{}".format(conveyor_id)
            message = "stopped"
            self.mqtt_client.publish(topic, message)

            self.set_output_register(reg, offset)

    def conveyor_forward(self, conveyor_id):
        """
                Drives the conveyor specified by conveyor_id forward by setting the forward bit and clearing the reverse bit.
                The analog outputs that control the speed of the treadmills are not changed.
                :param conveyor_id the index of the conveyor as character (see hardware documentation chapter 2.1.3)
                """
        with self.sem:
            # Necessary offset is calculated to address the correct word
            # Since the bit for forwards/backwards is always in the same offset on the treadmills, it is sufficient to determine the offset from one of the two
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)

            reg = self.get_output_register(offset)

            # Set forward bit and clear reverse bit
            reg[0] = set_bit(reg[0], bit_forward)
            reg[0] = reset_bit(reg[0], bit_backward)

            # Publish MQTT message
            topic = "Transport_out/conveyor/{}/direction".format(conveyor_id)
            message = "forward"
            self.mqtt_client.publish(topic, message)

            self.set_output_register(reg, offset)

    def conveyor_backward(self, conveyor_id: object) -> object:
        """
        Makes the conveyor specified by conveyor_id run backwards by clearing the forward bit and setting the reverse bit.
        The analog outputs that control the speed of the treadmills are not changed.
        :param conveyor_id the index of the conveyor as character (see hardware documentation chapter 2.1.3)
        """
        with self.sem:
            # Necessary offset is calculated to address the correct word
            # Since the bit for forwards/backwards is always in the same offset on the treadmills, it is sufficient to determine the offset from one of the two
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)

            reg = self.get_output_register(offset)

            # Clear forward bit and set reverse bit
            reg[0] = reset_bit(reg[0], bit_forward)
            reg[0] = set_bit(reg[0], bit_backward)

            # Publish MQTT message
            topic = "Transport_out/conveyor/{}/direction".format(conveyor_id)
            message = "forward"
            self.mqtt_client.publish(topic, message)

            self.set_output_register(reg, offset)

    def set_switch(self, switch_id, pos=0):
        """
                Sets the switch specified via switch_id to the position pos by first deleting all bits for the positions and
                then the bit for the position pos is set.
                :param switch_id the index of the switch as a character (see hardware documentation chapter 2.1.3)
                :param pos Position to which the switch is set (pos = 0 triggers reference travel)
                """
        with self.sem:
            # Since the bits for controlling a turnout have different offsets on some turnouts, the separate offset must be calculated for each bit.
            offset = [
                self.get_offset(self.INDEX.get(switch_id)[0]),  # Reference position
                self.get_offset(self.INDEX.get(switch_id)[1]),  # Position 1
                self.get_offset(self.INDEX.get(switch_id)[2]),  # Position 2
                self.get_offset(self.INDEX.get(switch_id)[3])  # Position 3
            ]
            bit = [
                self.get_bit(switch_id, 0),  # Reference position
                self.get_bit(switch_id, 1),  # Position 1
                self.get_bit(switch_id, 2),  # Position 2
                self.get_bit(switch_id, 3)  # Position 3
            ]

            # Deletes all bits for the switch position (if 2 or more bits were set at the same time, it would not be clear which position the switch should take)
            for i in range(4):
                reg = self.get_output_register(offset=offset[i])
                reg[0] = reset_bit(reg[0], bit[i])
                self.set_output_register(reg, offset=offset[i])

            # Sets the bit that the switch moves to the position pos
            reg = self.get_output_register(offset=offset[pos])
            reg[0] = set_bit(reg[0], bit[pos])
            self.set_output_register(reg, offset=offset[pos])

            # Publish MQTT message
            self.mqtt_client.publish("Transport_out", "Switch {} set to position {}".format(switch_id, pos))

    def set_seperator(self, seperator_id):
        """
        Sets the separator specified with separator_id.
        :param seperator_id index of the separator (see hardware documentation chapter 2.1.3)
        """
        with self.sem:
            # Offset is 5 for all separator bits (since all bits are between 64-79)
            offset = 5  # self.get_offset(INDEX.get(seperator_id)[0])
            bit_set = self.get_bit(seperator_id, 0)

            reg = self.get_output_register(offset)

            # Sets the bit to set the separator
            reg[0] = set_bit(reg[0], bit_set)

            # Publish message to MQTT
            self.mqtt_client.publish("Transport_out", "seperator/set", str(seperator_id))

            self.set_output_register(reg, offset)

    def reset_seperator(self, seperator_id):
        """
        Resets the separator specified with separator_id.
        :param seperator_id index of the separator (see hardware documentation chapter 2.1.3)
        """
        with self.sem:
            # Offset is 5 for all separator bits (since all bits are between 64-79)
            offset = 5  # self.get_offset(INDEX.get(seperator_id)[0])
            bit_set = self.get_bit(seperator_id, 0)

            reg = self.get_output_register(offset)

            # Clears the bit to set the separator
            reg[0] = reset_bit(reg[0], bit_set)

            self.set_output_register(reg, offset)

    def check_conveyor_workpiece_begin(self, conveyor_id):
        """
        Checks whether the sensor detects a workpiece at the beginning of the conveyor belt specified with conveyor_id.
        :param conveyor_id the index of the conveyor as character (see hardware documentation chapter 2.1.3)
        :returns boolean whether the sensor detects a workpiece
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
        bit_sensor_beginning = self.get_bit(conveyor_id, 0)
        detected = test_bit(self.get_input_register(offset)[0], bit_sensor_beginning)

        if detected:
            # Publish message to MQTT
            topic = "Transport_out/conveyor/{}/workpiece_detected".format(conveyor_id)
            message = "true"
            self.mqtt_client.publish(topic, message)

        return detected

    def check_conveyor_workpiece_end(self, conveyor_id):
        """
        Checks whether the sensor detects a workpiece at the end of the conveyor belt specified with conveyor_id.
        :param conveyor_id the index of the conveyor as character (see hardware documentation chapter 2.1.3)
        :returns boolean whether the sensor detects a workpiece
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(conveyor_id)[1])
        bit_sensor_end = self.get_bit(conveyor_id, 1)
        workpiece_end = test_bit(self.get_input_register(offset)[0], bit_sensor_end)
        if workpiece_end:
            self.mqtt_client.publish("Transport_out", "conveyor/{}/workpiece_end".format(conveyor_id),
                                     "Workpiece detected at end of conveyor")

        return workpiece_end

    def check_switch_position_reached(self, switch_id):
        """
        Checks whether the switch specified with switch_id has reached the desired position.
        :param switch_id the index of the switch as a character (see hardware documentation chapter 2.1.3)
        :returns boolean whether the turnout has reached the position
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[0])
        bit_pos_reached = self.get_bit(switch_id, 0)
        pos_reached = test_bit(self.get_input_register(offset)[0], bit_pos_reached)

        # Publish message to MQTT
        if pos_reached:
            self.mqtt_client.publish("Transport_out", "Switch {} has reached the desired position".format(switch_id))

        return pos_reached

    def check_switch_in_movement(self, switch_id):
        """
        Checks whether the switch specified with switch_id is in motion.
        :param switch_id the index of the switch as a character (see hardware documentation chapter 2.1.3)
        :returns boolean whether the turnout is in motion
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[1])
        bit_in_movement = self.get_bit(switch_id, 1)
        in_movement = test_bit(self.get_input_register(offset)[0], bit_in_movement)

        if in_movement:
            message = "Switch {} is in motion.".format(switch_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return in_movement

    def check_switch_workpiece(self, switch_id):
        """
        Checks whether there is a workpiece in the switch specified with switch_id.
        :param switch_id the index of the switch as a character (see hardware documentation chapter 2.1.3)
        :returns boolean whether there is a workpiece in the switch
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[2])
        bit_workpiece = self.get_bit(switch_id, 2)
        workpiece_present = test_bit(self.get_input_register(offset)[0], bit_workpiece)

        if workpiece_present:
            message = "Workpiece present at".format(switch_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return workpiece_present

    def check_switch_in_reference_position(self, switch_id):
        """
        Checks the homing bit, but cannot be used to determine end of homing.
        Method was only implemented for the sake of completeness.
        :param switch_id the index of the switch as a character (see hardware documentation chapter 2.1.3)
        :returns boolean the home position bit is set
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[3])
        bit_refposition = self.get_bit(switch_id, 3)
        workpiece_refposition = test_bit(self.get_input_register(offset)[0], bit_refposition)

        if workpiece_refposition:
            message = "Workpiece at reference".format(switch_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return workpiece_refposition

    def check_seperator_set(self, seperator_id):
        """
        Checks whether the separator specified with seperator_id is set.
        :param seperator_id index of the separator (see hardware documentation chapter 2.1.3)
        :returns boolean whether the separator is set
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[1])
        bit_getset = self.get_bit(seperator_id, 1)
        seperator_set = test_bit(self.get_input_register(offset)[0], bit_getset)

        if seperator_set:
            message = "Separator id is set".format(seperator_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return seperator_set

    def check_seperator_workpiece_behind(self, seperator_id):
        """
        Checks whether there is a workpiece behind the separator specified with seperator_id.
        :param seperator_id index of the separator (see hardware documentation chapter 2.1.3)
        :returns boolean Whether there is a workpiece behind the separator
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[2])
        bit_workpiece_behind = self.get_bit(seperator_id, 2)
        workpiece_behind = test_bit(self.get_input_register(offset)[0], bit_workpiece_behind)

        if workpiece_behind:
            message = "Checking whether workpiece behind the separator specified".format(seperator_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return workpiece_behind

    def check_seperator_workpiece_in_front(self, seperator_id):
        """
        Checks whether there is a workpiece in front of the separator specified with seperator_id.
        :param seperator_id index of the separator (see hardware documentation chapter 2.1.3)
        :returns boolean whether there is a workpiece in front of the separator
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[3])
        bit_workpiece_infront = self.get_bit(seperator_id, 3)
        workpiece_infront = test_bit(self.get_input_register(offset)[0], bit_workpiece_infront)

        if workpiece_infront:
            message = "Checking whether workpiece is in-front of the separator specified".format(seperator_id)
            self.mqtt_client.publish(self.mqtt_topic, message)

        return workpiece_infront

    def update_conveyor_speed(self):
        """
        Sets the analog outputs for controlling the conveyor speed to the values specified in the self.conveyor_speed map.
        """
        with self.read_write_sem:
            # Cancel automatic opening and closing of TCP connections, since many TCP packets are sent one after the other
            # and it is therefore better to open the connection once and then close it again.
            self.client.auto_close = False
            self.client.auto_open = False

            # Open the TCP connection
            self.client.open()

            self.client.write_single_register(8024, int("0x6000", 16))
            self.client.write_single_register(8024, int("0x3000", 16))

            self.client.write_single_register(8025, self.conveyor_speed.get('A'))
            self.client.write_single_register(8026, self.conveyor_speed.get('B'))
            self.client.write_single_register(8027, self.conveyor_speed.get('D'))
            self.client.write_single_register(8028, self.conveyor_speed.get('E'))

            self.client.write_single_register(8024, int("0x0100", 16))
            self.client.write_single_register(8024, int("0x0b00", 16))

            self.client.write_single_register(8025, self.conveyor_speed.get('G'))
            self.client.write_single_register(8026, self.conveyor_speed.get('H'))
            self.client.write_single_register(8027, self.conveyor_speed.get('I'))
            self.client.write_single_register(8028, self.conveyor_speed.get('K'))

            self.client.write_single_register(8024, int("0x0900", 16))

            self.client.write_single_register(8029, int("0x6000", 16))
            self.client.write_single_register(8029, int("0x3000", 16))

            self.client.write_single_register(8030, self.conveyor_speed.get('L'))
            self.client.write_single_register(8031, self.conveyor_speed.get('N'))
            self.client.write_single_register(8032, self.conveyor_speed.get('O'))
            self.client.write_single_register(8033, self.conveyor_speed.get('P'))

            self.client.write_single_register(8029, int("0x0100", 16))
            self.client.write_single_register(8029, int("0x0b00", 16))

            self.client.write_single_register(8030, self.conveyor_speed.get('T'))
            self.client.write_single_register(8031, self.conveyor_speed.get('U'))
            self.client.write_single_register(8032, self.conveyor_speed.get('V'))
            self.client.write_single_register(8033, self.conveyor_speed.get('W'))

            self.client.write_single_register(8029, int("0x0900", 16))

            # Closing the TCP connection
            self.client.close()

            self.client.auto_close = True
            self.client.auto_open = True

    def set_conveyor_speed(self, conveyor_id, speed):
        """
        Sets the speed of a treadmill to the given value.
        :param conveyor_id Index as character of the conveyor whose speed is to be set (see hardware documentation chapter 2.1.3)
        :param speed.  Speed of the treadmill as an integer between 0 (0%/0V) and 30000 (100%/10V)
        """
        with self.sem:
            self.conveyor_speed[conveyor_id] = speed
            self.update_conveyor_speed()

    def set_conveyor_speed(self, conveyor_id, speed):
        """
        Sets the speed of a treadmill to the given value.
        :param conveyor_id Index as character of the conveyor whose speed is to be set (see hardware documentation chapter 2.1.3)
        :param speed. Speed of the treadmill as an integer between 0 (0%/0V) and 30000 (100%/10V)
        """
        with self.sem:
            self.conveyor_speed[conveyor_id] = speed
            self.update_conveyor_speed()

    def set_conveyor_speed_all(self, speed):
        """
        Sets the speed of all treadmills to the given value.
        :param speed Speed of the treadmills as an integer between 0 (0%/0V) and 30000 (100%/10V)
        """
        with self.sem:
            for i in self.INDEX_CONVEYORS:
                self.conveyor_speed[i] = speed
            self.update_conveyor_speed()


out_1 = TransportOutputModule("192.168.200.236")
reg_val = out_1.get_input_register()
print(reg_val)



all_functions = methods(TransportOutputModule)[1:]

#print(all_functions)
out_1.get_input_register()
out_1.get_output_register()
print(out_1.check_conveyor_workpiece_end('B'))
mqtt_broker = "192.168.200.161"
mqtt_topic = "Transport_out"
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_broker)
mqtt_client.loop_start()

mqtt_client.publish("Transport_out", "conveyor/B/workpiece_end", "Workpiece detected at end of conveyor")
print(out_1.check_switch_position_reached('X'))
out_1.set_conveyor_speed("B", 0)
out_1.set_conveyor_speed("U", 0)

'''
for i in range(10):
    out_1.get_input_register()
    out_1.get_output_register()
    out_1.set_switch("C")
    print(out_1.get_input_register())
'''