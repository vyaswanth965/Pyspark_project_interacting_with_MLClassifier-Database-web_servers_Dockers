import logging
import logging.config
import json

class Logger():
    def getLogger(self, moduleName):
        # LOGGING_CONFIG = None
        # config = None
        # with open('logger/loggerconfig.json') as json_file:  
        #     config = json.load(json_file)

        logging.config.dictConfig(
            {
            "version": 1,
            "disable_existing_loggers": "False",
            "formatters": {
                "console": {
                    "format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.FileHandler",
                    "formatter": "console",
                    "filename": "logs.txt"
                }
            },
            "loggers": {
                "": {
                    "level": "INFO",
                    "handlers": ["console"]
                }
            }
        })

        logger = logging.getLogger(moduleName)
        return logger




