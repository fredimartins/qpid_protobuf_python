from Qpid import QpidConnection
from mxt1xx_pb2 import *
from commands_pb2 import *
from QpidTypes import *
from qpid.messaging import *

#doc http://qpid.apache.org/releases/qpid-0.14/apis/python/html/
#examples https://developers.google.com/protocol-buffers/docs/pythontutorial


qpidCon = QpidConnection('192.168.0.78', '5672', 'fila_dados_ext', 'mxt_command_qpid')

while not(qpidCon.start()):
    print('Trying to reconnect')

response_received = True;

def mxt1xx_output_control(activate, pos, qpidCon):
    activate = not activate
    activate = int(activate == True)
    cmd = u_command()
    cmd.protocol = pos.firmware.protocol
    cmd.serial = pos.firmware.serial
    cmd.id = 'Controla Saida ' + str(pos.firmware.serial)
    cmd.type = 51
    cmd.attempt = 50
    cmd.timeout = '2020-12-31 00:00:00'
    cmd.handler_type = 2
    cmd.transport = 'GPRS'

    parameter = cmd.parameter.add()
    parameter.id = 'SET_OUTPUT'
    parameter.value = '1'

    parameter = cmd.parameter.add()
    parameter.id = 'SET OUTPUT 1'
    parameter.value = str(activate)

    parameter = cmd.parameter.add()
    parameter.id = 'SET OUTPUT 2'
    parameter.value = str(activate)

    parameter = cmd.parameter.add()
    parameter.id = 'SET OUTPUT 3'
    parameter.value = str(activate)

    parameter = cmd.parameter.add()
    parameter.id = 'SET OUTPUT 4'
    parameter.value = str(activate)

    message = Message(subject="PB_COMMAND", content=cmd.SerializeToString())
    qpidCon.sender.send(message)
    return False


while(1):
    message = qpidCon.receiver.fetch()
    subject = message.subject
    print (message.subject + ' received')

    if subject == QpidSubjectType.qpid_st_pb_mxt1xx_pos:
        pos = mxt1xx_u_position()
        pos.ParseFromString(message.content)
        print (str(pos.firmware.protocol) + ':' + str(pos.firmware.serial) + ':' + str(pos.firmware.memory_index))
        qpidCon.session.acknowledge()
        if response_received:
            response_received = mxt1xx_output_control(pos.hardware_monitor.outputs.output_1, pos, qpidCon);

    if subject == QpidSubjectType.qpid_st_pb_command_response:
        res = u_command_response()
        res.ParseFromString(message.content)
        if res.status == 5:
            print('Command response: Success')
            response_received = True
        else:
            print('Command response: ' + str(res.status))
    else:
        qpidCon.session.acknowledge()