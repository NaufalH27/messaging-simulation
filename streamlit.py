
event_origin = UTCDateTime(2019, 3, 11, 15, 14, 0)
client = Client(SEEDLINK_ENDPOINT)
print(event_origin)

st = client.get_waveforms("JP", "JMM", "", "JSD", event_origin, event_origin + 20)