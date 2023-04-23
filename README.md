# Netserve
## Installation
```bash
# Clone repo and navigate to folder
> python -m venv .env
> source .env/bin/activate
> pip install -r requirements.txt 
```

### Link client/server scripts
```bash
# Navigate to repo folder
> mkdir -p ~/.local/bin
> ln -s ./client.sh ~/.local/bin/netserve-client
> ln -s ./server.sh ~/.local/bin/netserve-server
> chmod a+x client.sh server.sh
```

## Usage
### Starting server
```bash
> source .env/bin/activate # Only needed once per terminal session
> python server.py
```

#### When linked scripts
```bash
> netserve-server
```

### Using client
Refer to command-line help
```bash
> source .env/bin/activate # Only needed once per terminal session
> python client.py --help
```

#### When linked scripts
```bash
> netserve-client --help
```

