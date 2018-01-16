# Admin UI

## Installation

   1. Quick Start
      1. cp env.sample .env
      2. *EDIT* .env
      3. *CHANGE* bigdawg-postgres-catalog to localhost or 192.168.99.100 or ??
      4. *INSTALL* flask (see below)
      5. export FLASK_APP=app.py
      6. flask run --host=0.0.0.0
      
### Installation of flask

This application should work on both python 2.7 and 3

You can check which version of python you have installed by doing:

   * python --version

You may want to install virtualenv to manage environments first:

   * pip install virtualenv
   * virtualenv -p /usr/bin/python venv

Then each time you want to use the program, do this first:

   * source venv/bin/activate

Now install the requirements:

   * pip install -r requirements.txt

Now test that flask is installed:

   * flask --version

You should see the something like one of the following:

```
Flask 0.12
Python 2.7.14 (default, Sep 23 2017, 22:06:14) 
[GCC 7.2.0]
```

or

```
Flask 0.12
Python 3.6.3 (default, Oct  3 2017, 21:45:48) 
[GCC 7.2.0]
```
 
