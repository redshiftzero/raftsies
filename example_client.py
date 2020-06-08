from raftsies.client import RaftClient


client = RaftClient('localhost', 15001)
client.set('tee', 'hee')
v = client.get('tee')
client.delete('tee')
client.get('notakey')
client.close()
