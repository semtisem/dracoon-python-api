
  <h3 align="center">DRACOON-PYTHON-API</h3>

  <p align="center">
    Python connector to DRACOON API
    <br />
    <a href="https://github.com/unbekanntes-pferd/DRACOON-PYTHON-API"><strong>Explore the docs »</strong></a>
    <br />
    <a href="https://github.com/unbekanntes-pferd/DRACOON-PYTHON-API/issues">Report Bug</a>
  </p>
</p>

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)



<!-- ABOUT THE PROJECT -->
## About The Project
__Disclaimer: this is an unofficial repo and is not supported by DRACOON__<br>
This package provides a connector to DRACOON API. 
DRACOON is a cloud storage product / service (SaaS) by DRACOON GmbH (http://dracoon.com). 
DRACOON API documentation can be found here (Swagger UI):
https://dracoon.team/api/


### Built With

* [Python 3.7.3](https://www.python.org/)
* [requests module](https://requests.readthedocs.io/en/master/)
* [aiohttp module](https://docs.aiohttp.org/en/stable/)

<!-- GETTING STARTED -->
## Getting Started

To get started, create a virtual environment in Python and install the dracoon package:
```
virtualenv <DIR>
source <DIR>/bin/activate 
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps dracoon-UNBEKANNTES-PFERD
```

### Prerequisites

You will need a working Python 3 installation - check your version:
* Python
```
python3 --version
```

### Installation

1. Install the package from TestPyPi
```
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps dracoon-UNBEKANNTES-PFERD
```

Please note: this package will also be provided via official PyPi soon.

<!-- USAGE EXAMPLES -->
## Usage
### Import required modules
```
from dracoon import core, users
```

Modules are named after API endpoints (see documentation for further details).<br>
_Exception: core module - this is required to create Dracoon object and to send authenticated requests to the API._

### Object creation
```
my_dracoon = core.Dracoon(clientID, clientSecret)
my_dracoon.set_URLs(baseURL)
```
Please note: providing a client secret is optional (in order to use with OAuth apps that don't have one).
* _clientID_; please register your OAuth app or use dracoon_legacy_scripting
* _clientSecret_; please register your OAuth app or use dracoon_legacy_scripting
* _baseURL_: Your DRACOON URL instance (e.g. https://dracoon.team)


### Authentication
```
login_response = my_dracoon.basic_auth(username, password)
```
Please note: you can only authenticate if OAuth app is correctly configured. Only local accounts can be used via password flow.

### Send requests

1. First you will need to build a request with specific parameters:
```
r = users.get_users(offset=0, filter=f)
```

Please note: 
* GET requests are limited to returning 500 items. Therefore all such requests contain an offset parameter (default is 0)
* Providing a filter is optional - see API documentation and examples on usage
* Sorting not implemented - sorting results should occur via client

2. you can then send the request as an authenticated user
```
user_response = my_dracoon.get(r)
```
Supported request types:
* GET (object.get(request))
* POST (oject.post(request))
* PUT (object.put(reqest))
* DELETE (object.delete(request))

_For examples, check out the example files:_<br>

* [DRACOON authentication](/authentication_example.py)
* [Export user list to CSV](/user_csv_example.py)
* [Import users from CSV](/import_csv_example.py)

### Send async requests

1. First you will need to build a request with specific parameters:
```
r = users.get_users(offset=0, filter=f)
```

Please note: 
* GET requests are limited to returning 500 items. Therefore all such requests contain an offset parameter (default is 0)
* Providing a filter is optional - see API documentation and examples on usage
* Sorting not implemented - sorting results should occur via client

2. you will need to call async requests inside an async function (e.g. main()) and pass a client session
```
         async with aiohttp.ClientSession() as session:
             user_response = await my_dracoon.async_get(r, session)
```
Supported request types:
* GET (object.async_get(request, session))
* POST (oject.async_post(request, session))
* PUT (object.async_put(reqest, session))
* DELETE (object.async_delete(request, session))

_For examples, check out the example file:_<br>

* [Async requests](/async_requests_example.py)

<!-- ROADMAP -->
## Roadmap

* Implement public API to upload files 
* Implement missing API endpoint (selection)
    * shares
    * eventlog 
    * uploads
    * public
* Distribute package via official PyPi

<!-- LICENSE -->
## License

Distributed under the Apache License. See [LICENSE](/LICENSE) for more information.
