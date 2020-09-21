import pymsteams
from creds import teams_hook

myTeamsMessage = pymsteams.connectorcard(teams_hook)



def sendMessage(title, message):
    myTeamsMessage.title(title)
    myTeamsMessage.text(message)

    myTeamsMessage.send()

