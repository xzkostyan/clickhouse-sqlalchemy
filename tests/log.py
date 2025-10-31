from logging.config import dictConfig


def configure(level):
    fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {"format": fmt},
            },
            "handlers": {
                "default": {
                    "level": level,
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["default"],
                    "level": level,
                    "propagate": True,
                },
            },
        }
    )
