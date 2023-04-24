#!/usr/bin/python3
import rich
import sys
from rich.console import Console
from rich.columns import Columns

Topics = {
  'RFSTA': '#0cf25d', # raft state
  'STACH': '#ff81d0', # state change
  'TRMCH': '#ff81d0', # term change

  'ELECT': 'yellow',  # election
  'VOTE ': 'yellow',  # vote
  'HBEAT': 'yellow',  # heart beat

  'APPND': '#ff9933', # append entries
  'AGREE': '#ff9933', # agreement
  'SNDEN': 'blue',    # send entries
  'COMIT': 'white',   # commit entries
  'PERST': '#d92525', # persistence
  'SSHOT': '#00bfae', # snapshot
  'LOCK ': 'red',      # locking

  "CAGET": '#a6bc09',
  "CAPUT": '#a6bc09',
  "REGET": '#a6bc09',
  "REGET": '#a6bc09',
}

log = sys.stdin
if (len(sys.argv) > 1):
  log = open(sys.argv[1], "r")

console = Console()
consoleWidth = console.size.width
ncolumns = 3

for line in log:
  try:
    time = int(line[:6])
    topic = line[7:12]
    color = Topics[topic]

    peer = int(line[18])
    msg = line[20:].strip()
    msg = f"[{color}]{msg}[/{color}]"
    
    cols = ["" for _ in range(ncolumns)]
    cols[peer] = msg
    columnWidth = int(consoleWidth / ncolumns)
    cols = Columns(cols, width=columnWidth-1, equal=True, expand=True)

    rich.print(cols)
  except:
    print('-' * consoleWidth)
