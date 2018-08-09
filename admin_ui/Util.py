"""
Utilities

See example usage at the bottom.
"""
import json, numbers
import os
import psycopg2

class Util:
    @staticmethod
    def error_msg(msg):
        msg = str(msg)
        print("error: " + msg)
        return json.JSONEncoder().encode({ "success": False, "error": msg });

    @staticmethod
    def success_msg():
        return json.JSONEncoder().encode({ "success": True })
