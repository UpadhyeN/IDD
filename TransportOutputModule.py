from pyModbusTCP.client import ModbusClient
from pyModbusTCP.utils import get_bits_from_int
from pyModbusTCP.utils import set_bit
from pyModbusTCP.utils import reset_bit
from pyModbusTCP.utils import test_bit
from time import sleep

from multiprocessing import BoundedSemaphore



class TransportOutputModule:
    #Konstanten
    DIGITAL_INPUT_STARTING_ADDRESS = 8001
    DIGITAL_OUTPUT_STARTING_ADDRESS = 8018


    INDEX_CONVEYORS = ['A', 'B', 'D', 'E', 'G', 'H', 'I', 'K', 'L', 'N', 'O', 'P', 'T', 'U', 'V', 'W']
    INDEX_SWITCHES = ['C', 'F', 'J', 'M', 'Q', 'R', 'S', 'X']
    #Mappt die Idize der Laufbänder, Weichen und Vereinzeler auf die Digitalen Ein- und Ausgänge mit denen diese verbunden sind
    #Abgeleitet aus den Tabellen 2.1, 2.2, 2.3 und 2.4 aus Kapitel 2.1.3 der Hardwaredokumentation
    INDEX = {
        #Laufbänder Idize: [Bit-Vorwärts/Sensor Anfang, Bitrückwärts/Sensor Ende]
        'A' : [ 0,  1],
        'B' : [ 2,  3],
        'D' : [ 8,  9],
        'E' : [10, 11],
        'G' : [16, 17],
        'H' : [18, 19],
        'I' : [20, 21],
        'K' : [26, 27],
        'L' : [28, 29],
        'N' : [34, 35],
        'O' : [36, 37],
        'P' : [38, 39],
        'T' : [52, 53],
        'U' : [54, 55],
        'V' : [56, 57],
        'W' : [58, 59],

        #Weichen Indize : [Referenzfahrt/Pos. Erreicht, Pos.1/In Bewegung, Pos.2/Wstk in Weiche, Pos.3/Referenzpos]
        'C' : [ 4,  5,  6,  7],
        'F' : [12, 13, 14, 15],
        'J' : [22, 23, 24, 25],
        'M' : [30, 31, 32, 33],
        'Q' : [40, 41, 42, 43],
        'R' : [44, 45, 46, 47],
        'S' : [48, 49, 50, 51],
        'X' : [60, 61, 62, 63],

        #Vereinzeler Indize : [Setzen, Gesetzt, Werkstück hinter Vereinzeler, Werkstück vor Vereinzeler]
        'V1' : [64, 64, 65, 66],
        'V2' : [65, 67, 68, 69],
        'V3' : [66, 70, 71, 72]

    }

    
    def __init__(self,ip_addr, read_write_sem = BoundedSemaphore(value=1)):
        """
        Konstruktor des TranporAusgangModuls.

        :param ip_addr Ip-Adresse des Modbus Knoten, welche für die Bearbeiten Station zuständig ist (String)
        :param read_write_sem Semaphore die übergeben werden kann, wenn nicht erwünscht ist, dass 2 Module gleichzeitig read/write Befehle schicken
        """
        try:
            #Erzeugt eine Verbindung zum Modbus mit der ip_addr
            self.client = ModbusClient(host=ip_addr, auto_open=True, auto_close=True)
        except ValueError:
            print("Error with host param")
        
        #Semaphore, die dafür sorgt, dass immer nur ein Thread gleichzeitig auf die In- und Outputs des Modbus zugreifen kann
        self.sem = BoundedSemaphore(value=1)

        self.read_write_sem = read_write_sem

        #Speed der Laufbänder 0 = 0V/0% | 30000 = 10V/100%
        #Standardmäßig alle Speeden auf 0%
        self.conveyor_speed = {
            'A' : 0,
            'B' : 0,
            'D' : 0, 
            'E' : 0, 
            'G' : 0, 
            'H' : 0, 
            'I' : 0, 
            'K' : 0, 
            'L' : 0, 
            'N' : 0, 
            'O' : 0, 
            'P' : 0, 
            'T' : 0, 
            'U' : 0, 
            'V' : 0, 
            'W' : 0
            }

    def get_output_register(self, offset = 0, amount = 1):
        """
        Gibt die Output Register des Modbus zurück.

        :param offset Offset zur DIGITAL_OUTPUT_STARTING_ADDRESS
        :param amount Amount der Register die ausgelesen werden
        :returns Liste der gelesenen Register (oder garnichts wenn lesen fehlschlägt)
        :rtype list of int or none
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.read_holding_registers(reg_addr=self.DIGITAL_OUTPUT_STARTING_ADDRESS + offset,reg_nb = amount)
            return result

    def get_input_register(self, offset = 0, amount = 1):
        """
        Gibt die Input Register des Modbus zurück.

        :param offset Offset zur DIGITAL_INPUT_STARTING_ADDRESS
        :param amount Amount der Register die ausgelesen werden
        :returns Liste der gelesenen Register (oder garnichts wenn lesen fehlschlägt)
        :rtype list of int or none
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.read_holding_registers(reg_addr=self.DIGITAL_INPUT_STARTING_ADDRESS + offset,reg_nb = amount)
            return result

    def set_output_register(self, register, offset = 0):
        """
        Überschreibt das Output Register des Modbus.

        :param register List of int die in das Register geschrieben werden soll
        :param offset Offset zur DIGITAL_OUTPUT_STARTING_ADDRESS
        """
        with self.read_write_sem:
            result = None
            while result == None:
                result = self.client.write_multiple_registers(self.DIGITAL_OUTPUT_STARTING_ADDRESS + offset, register)

    def get_offset(self, bit_nr):
        """
        Berechnet den Offset für get_input_register()/get_output_register()/set_output_register() anhand der übergebenen bit_nr.
        :param bit_nr Nummer des Bits für den der Offset errechnet werden soll
        :returns offset des bits
        :rtype int
        """
        #Offset 0 -> 16 - 31
        #Offset 1 ->  0 - 15
        #Offset 2 -> 48 - 63
        #Offset 3 -> 32 - 47
        #Offset 5 -> 64 - 79 
        #(Theoretisch 79, aber 67 ist das höchste benötigte bit, deswegen wird Offset 4 eigentlich nicht benötigt)
        #Offset Reihenfolge liegt an der Little Endian Reihenfolge
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
        Gibt die Bit Nummer zurück die Notwendig ist um innerhalb eines Words auf das richtige Bit zuzugreifen.
        :param index Index des Anzusprechenden Moduls/Gerätes (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :param nr Nummer der Funktion für die das Bit benötigt wird (Siehe Kommatare in der Map INDEX)
        :returns Nummer des Bits
        :rtype int
        """
        #Nummer wird Modulo 16 gerechnet, da alle 16 Bit ein neues word anfängt, bei dem wieder mit 0 angefangen wird zu adressieren
        return self.INDEX.get(index)[nr] % 16


    def conveyor_stop(self, conveyor_id):
        """
        Hält das Laufband, welches über conveyor_id angegeben wurde an, indem die Bits für Vor- und Rückwärts fahren gelöscht werden.
        Die analogen Ausgänge, die die Speed der Laufbänder steuern, werden nicht verändert.
        :param conveyor_id der Index des Laufbands als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        """
        with self.sem:
            #Nötiger Offset wird berechnet um das korrekte word zu adressieren
            #Da bei den Laufbändern das Bit für Vor/Zurück immer im selben Offset liegen reicht es von einem der Beiden des Offset zu bestimmen
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)

            reg = self.get_output_register(offset)
            #Bits für Vor- und Rückwärts fahren löschen damit das Band anhält
            reg[0] = reset_bit(reg[0], bit_forward)
            reg[0] = reset_bit(reg[0], bit_backward)

            self.set_output_register(reg, offset)                

    def all_conv_stop(self):
        pass
        
        
    def conveyor_forward(self, conveyor_id):
        """
        Lässt das Laufband, welches über conveyor_id angegeben wurde, vorwärts fahren, indem das Bit für Vorwärts gesetzt und das Bit für Rückwärts fahren gelöscht wird.
        Die analogen Ausgänge, die die Speed der Laufbänder steuern, werden nicht verändert.
        :param conveyor_id der Index des Laufbands als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        """
        with self.sem:
            #Nötiger Offset wird berechnet um das korrekte word zu adressieren
            #Da bei den Laufbändern das Bit für Vor/Zurück immer im selben Offset liegen reicht es von einem der Beiden des Offset zu bestimmen
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)
            
            reg = self.get_output_register(offset)
            
            #Bit für Vorwärts setzen und Bit für Rückwärts löschen
            reg[0] = set_bit(reg[0], bit_forward)
            reg[0] = reset_bit(reg[0], bit_backward)

            self.set_output_register(reg, offset)
            
    def conveyor_backward(self, conveyor_id):
        """
        Lässt das Laufband, welches über conveyor_id angegeben wurde, rückwärts fahren, indem das Bit für Vorwärts gelöscht und das Bit für Rückwärts fahren gesetzt wird.
        Die analogen Ausgänge, die die Speed der Laufbänder steuern, werden nicht verändert.
        :param conveyor_id der Index des Laufbands als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        """
        with self.sem:
            #Nötiger Offset wird berechnet um das korrekte word zu adressieren
            #Da bei den Laufbändern das Bit für Vor/Zurück immer im selben Offset liegen reicht es von einem der Beiden des Offset zu bestimmen
            offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
            bit_forward = self.get_bit(conveyor_id, 0)
            bit_backward = self.get_bit(conveyor_id, 1)
            
            reg = self.get_output_register(offset)
            
            #Bit für Vorwärts löschen und Bit für Rückwärts setzen
            reg[0] = reset_bit(reg[0], bit_forward)
            reg[0] = set_bit(reg[0], bit_backward)

            self.set_output_register(reg, offset)

    def set_switch(self, switch_id , pos = 0):
        """
        Stellt die Weiche, die über switch_id angegeben wurde, auf die Position pos, indem erst alle Bits für die Positionen gelöscht werden und
        danach das Bit für die Position pos gesetzt wird.
        :param switch_id der Index der Weiche als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :param pos Position auf welche die Weiche gestellt wird (pos = 0 löst Referenzfahrt aus)
        """
        with self.sem:
            #Da bei einigene Weichen die Bits zur ansteuerung einer Weiche unterschiedliche Offsets haben muss hier zu jedem Bit der eigene Offset berechnet werden.
            offset = [
                self.get_offset(self.INDEX.get(switch_id)[0]), #Referenz Fahrt
                self.get_offset(self.INDEX.get(switch_id)[1]), #Position 1
                self.get_offset(self.INDEX.get(switch_id)[2]), #Position 2
                self.get_offset(self.INDEX.get(switch_id)[3])  #Position 3
            ]
            bit = [
                self.get_bit(switch_id, 0), #Referenz Fahrt
                self.get_bit(switch_id, 1), #Position 1
                self.get_bit(switch_id, 2), #Position 2
                self.get_bit(switch_id, 3)  #Position 3
            ]

            #Löscht alle Bits zur Weichenstellung (wenn 2 oder mehr Bits gleichzeitig gesetzt wären, wäre nicht eindeutig welche Position die weiche einnehmen soll)
            for i in range(4):
                reg = self.get_output_register(offset=offset[i])
                reg[0] = reset_bit(reg[0], bit[i])
                self.set_output_register(reg, offset=offset[i])

            #Setzt das Bit, dass die Weiche an die Position pos fährt
            reg = self.get_output_register(offset=offset[pos])
            reg[0] = set_bit(reg[0], bit[pos])
            self.set_output_register(reg, offset=offset[pos])

    def set_seperator(self, seperator_id):
        """
        Setzt den vereinzeler, welcher mit seperator_id angegeben wurde.
        :param seperator_id Index des Vereinzelers (Siehe Hardwaredokumentation Kapitel 2.1.3)
        """
        with self.sem:
            #Offset ist für alle Vereinzeler Bits 5 (da alle Bits zwischen 64-79 liegen)
            offset = 5 #self.get_offset(INDEX.get(seperator_id)[0])
            bit_setzen = self.get_bit(seperator_id, 0)
            
            reg = self.get_output_register(offset)

            #Setzt das Bit um den Vereinzeler zu setzen
            reg[0] = set_bit(reg[0], bit_setzen)

            self.set_output_register(reg, offset)

    def reset_seperator(self, seperator_id):
        """
        Setzt den vereinzeler zurück, welcher mit seperator_id angegeben wurde.
        :param seperator_id Index des Vereinzelers (Siehe Hardwaredokumentation Kapitel 2.1.3)
        """
        with self.sem:
            #Offset ist für alle Vereinzeler Bits 5 (da alle Bits zwischen 64-79 liegen) 
            offset = 5 #self.get_offset(INDEX.get(seperator_id)[0])
            bit_setzen = self.get_bit(seperator_id, 0)

            reg = self.get_output_register(offset)

            #Löscht das Bit um den Vereinzeler zu setzen
            reg[0] = reset_bit(reg[0], bit_setzen)

            self.set_output_register(reg, offset)

    def check_conveyor_workpiece_begin(self, conveyor_id):
        """
        Überprüft, ob der Sensor am Anfang des Laufbandes, welches mit conveyor_id angegeben wurde, ein Werkstück erkennt.
        :param conveyor_id der Index des Laufbands als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob der Sensor ein Werkstück erkennt
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(conveyor_id)[0])
        bit_sensor_anfang = self.get_bit(conveyor_id, 0)

        return test_bit(self.get_input_register(offset)[0], bit_sensor_anfang)

    def check_conveyor_workpiece_end(self, conveyor_id):
        """
        Überprüft, ob der Sensor am Ende des Laufbandes, welches mit conveyor_id angegeben wurde, ein Werkstück erkennt.
        :param conveyor_id der Index des Laufbands als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob der Sensor ein Werkstück erkennt
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(conveyor_id)[1])
        bit_sensor_ende = self.get_bit(conveyor_id, 1)

        return test_bit(self.get_input_register(offset)[0], bit_sensor_ende)

    def check_switch_position_reached(self, switch_id):
        """
        Überprüft, ob die Weiche, welche mit switch_id angegeben wurde, die gewünschte Position erreicht hat.
        :param switch_id der Index der Weiche als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob die weiche die Position erreicht hat
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[0])
        bit_pos_erreicht = self.get_bit(switch_id, 0)

        return test_bit(self.get_input_register(offset)[0], bit_pos_erreicht)

    def check_switch_in_movement(self, switch_id):
        """
        Überprüft, ob die Weiche, welche mit switch_id angegeben wurde, in Bewegung ist.
        :param switch_id der Index der Weiche als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob die Weiche in Bewegung ist
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[1])
        bit_in_bewegung = self.get_bit(switch_id, 1)

        return test_bit(self.get_input_register(offset)[0], bit_in_bewegung)

    def check_switch_workpiece(self, switch_id):
        """
        Überprüft, ob sich in der Weiche, welche mit switch_id angegeben wurde, ein Werkstueck befindet.
        :param switch_id der Index der Weiche als Character (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob sich in der Weiche ein Werkstueck befindet
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[2])
        bit_werkstueck = self.get_bit(switch_id, 2)

        return test_bit(self.get_input_register(offset)[0], bit_werkstueck)

    def check_switch_in_reference_position(self, switch_id):
        """
        e
        """
        offset = self.get_offset(self.INDEX.get(switch_id)[3])
        bit_referenzposition = self.get_bit(switch_id, 3)

        return test_bit(self.get_input_register(offset)[0], bit_referenzposition)

    def check_seperator_set(self, seperator_id):
        """
        Überprüft, ob der Vereinzeler, welcher mit seperator_id angegeben wurde, gesetzt ist.
        :param seperator_id Index des Vereinzelers (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob der Vereinzeler gesetzt ist
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[1])
        bit_gesetzt = self.get_bit(seperator_id, 1)

        return test_bit(self.get_input_register(offset)[0], bit_gesetzt)

    def check_seperator_workpiece_behind(self, seperator_id):
        """
        Überprüft, ob sich hinter dem Vereinzeler, welcher mit seperator_id angegeben wurde, ein Werkstüeck befindet.
        :param seperator_id Index des Vereinzelers (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob sich hinter dem Vereinzeler ein Werkstück befindet
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[2])
        bit_werkstueck_hinten = self.get_bit(seperator_id, 2)

        return test_bit(self.get_input_register(offset)[0], bit_werkstueck_hinten)

    def check_seperator_workpiece_in_front(self, seperator_id):
        """
        Überprüft, ob sich vor dem Vereinzeler, welcher mit seperator_id angegeben wurde, ein Werkstüeck befindet.
        :param seperator_id Index des Vereinzelers (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :returns boolean ob sich vor dem Vereinzeler ein Werkstück befindet
        :rtype bool
        """
        offset = self.get_offset(self.INDEX.get(seperator_id)[3])
        bit_werkstueck_vorne = self.get_bit(seperator_id, 3)

        return test_bit(self.get_input_register(offset)[0], bit_werkstueck_vorne)

    def update_conveyor_speed(self):
        """
        Setzt die Analogen Ausgänge zum regeln der Laufbandspeed auf die Werte, die in der Map self.conveyor_speed angegeben werden.
        """
        with self.read_write_sem:
            #Automatisches öffnen und schließen von TCP verbindungen aufheben, da hier viele TCP Pakete nacheinander gesendet werden
            #und es somit besser ist einmal die Verbindung zu öffnen und danach wieder zu schließen.
            self.client.auto_close = False
            self.client.auto_open = False

            #Öffnen der TCP Verbindung
            self.client.open()

            self.client.write_single_register(8024, int("0x6000",16))
            self.client.write_single_register(8024, int("0x3000",16))

            self.client.write_single_register(8025, self.conveyor_speed.get('A'))
            self.client.write_single_register(8026, self.conveyor_speed.get('B'))
            self.client.write_single_register(8027, self.conveyor_speed.get('D'))
            self.client.write_single_register(8028, self.conveyor_speed.get('E'))

            self.client.write_single_register(8024, int("0x0100",16))
            self.client.write_single_register(8024, int("0x0b00",16))

            self.client.write_single_register(8025, self.conveyor_speed.get('G'))
            self.client.write_single_register(8026, self.conveyor_speed.get('H'))
            self.client.write_single_register(8027, self.conveyor_speed.get('I'))
            self.client.write_single_register(8028, self.conveyor_speed.get('K'))

            self.client.write_single_register(8024, int("0x0900",16))


            self.client.write_single_register(8029, int("0x6000",16))
            self.client.write_single_register(8029, int("0x3000",16))

            self.client.write_single_register(8030, self.conveyor_speed.get('L'))
            self.client.write_single_register(8031, self.conveyor_speed.get('N'))
            self.client.write_single_register(8032, self.conveyor_speed.get('O'))
            self.client.write_single_register(8033, self.conveyor_speed.get('P'))

            self.client.write_single_register(8029, int("0x0100",16))
            self.client.write_single_register(8029, int("0x0b00",16))

            self.client.write_single_register(8030, self.conveyor_speed.get('T'))
            self.client.write_single_register(8031, self.conveyor_speed.get('U'))
            self.client.write_single_register(8032, self.conveyor_speed.get('V'))
            self.client.write_single_register(8033, self.conveyor_speed.get('W'))

            self.client.write_single_register(8029, int("0x0900",16))

            #Schließen der TCP Verbindung
            self.client.close()

            self.client.auto_close = True
            self.client.auto_open = True

    def set_conveyor_speed(self, conveyor_id, speed):
        """
        Setzt die Speed eines Laufbands auf den übergebenen Wert.
        :param conveyor_id Index als Character des Laufbands, dessen Speed gesetzt werden soll (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :param speed Speed des Laufbandes als Integer zwischen 0 (0%/0V) und 30000 (100%/10V)
        """
        with self.sem:
            self.conveyor_speed[conveyor_id] = speed
            self.update_conveyor_speed()

    def set_conveyor_speed(self, conveyor_id, speed):
        """
        Setzt die Speed eines Laufbands auf den übergebenen Wert.
        :param conveyor_id Index als Character des Laufbands, dessen Speed gesetzt werden soll (Siehe Hardwaredokumentation Kapitel 2.1.3)
        :param speed Speed des Laufbandes als Integer zwischen 0 (0%/0V) und 30000 (100%/10V)
        """
        with self.sem:
            self.conveyor_speed[conveyor_id] = speed
            self.update_conveyor_speed()

    def set_conveyor_speed_all(self, speed):
        """
        Setzt die Speed aller Laufbänder auf den übergebenen Wert.
        :param speed Speed der Laufbänder als Integer zwischen 0 (0%/0V) und 30000 (100%/10V)
        """
        with self.sem:
            for i in self.INDEX_CONVEYORS:
                self.conveyor_speed[i] = speed
            self.update_conveyor_speed()
