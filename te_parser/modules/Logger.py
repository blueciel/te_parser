import logging
import logging.handlers
import os


class Setting:
    """로거 셋팅 클래스
        ::
            Setting.LEVEL = logging.INFO  # INFO 이상만 로그 작성
    """
    LEVEL = logging.DEBUG
    # print(os.path.abspath(__file__))
    # print(os.path.dirname(os.path.abspath(__file__)))
    # print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    FILENAME = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "te.log")
    MAX_BYTES = 10 * 1024 * 1024
    BACKUP_COUNT = 10
    FORMAT = "%(asctime)s[%(levelname)8s][%(name)-10s,%(filename)s,%(funcName)s(%(lineno)3s)] %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def make_logger(name):
    """파일 로그 클래스
        :param name: 로그 이름
        :type name: str
        :return: 로거 인스턴스
        ::
            logger = Logger(__name__)
            logger.info('info 입니다')
    """
    # 로거 & 포매터 & 핸들러 생성
    logger_inst = logging.getLogger(name)
    # 핸들러 존재 확인
    if len(logger_inst.handlers) > 0:
        return logger_inst  # 로거 이미 존재

    os.path.exists(os.path.dirname(Setting.FILENAME)) or os.mkdir(os.path.dirname(Setting.FILENAME))

    formatter = logging.Formatter(Setting.FORMAT, datefmt=Setting.DATE_FORMAT)
    steam_handler = logging.StreamHandler()
    file_handler = logging.handlers.RotatingFileHandler(
        filename=Setting.FILENAME,
        maxBytes=Setting.MAX_BYTES,
        backupCount=Setting.BACKUP_COUNT)

    # 핸들러 & 포매터 결합
    steam_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # 로거 & 핸들러 결합
    logger_inst.addHandler(steam_handler)
    logger_inst.addHandler(file_handler)

    # 로거 레벨 설정
    logger_inst.setLevel(Setting.LEVEL)

    return logger_inst


def make_file_logger(name):
    """파일 로그 클래스
        :param name: 로그 이름
        :type name: str
        :return: 로거 인스턴스
        ::
            logger = Logger(__name__)
            logger.info('info 입니다')
    """
    # 로거 & 포매터 & 핸들러 생성
    logger_inst = logging.getLogger(name)
    # 핸들러 존재 확인
    if len(logger_inst.handlers) > 0:
        return logger_inst  # 로거 이미 존재

    os.path.exists(os.path.dirname(Setting.FILENAME)) or os.mkdir(os.path.dirname(Setting.FILENAME))

    formatter = logging.Formatter(Setting.FORMAT, datefmt=Setting.DATE_FORMAT)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=Setting.FILENAME,
        maxBytes=Setting.MAX_BYTES,
        backupCount=Setting.BACKUP_COUNT)

    # 핸들러 & 포매터 결합
    file_handler.setFormatter(formatter)

    # 로거 & 핸들러 결합
    logger_inst.addHandler(file_handler)

    # 로거 레벨 설정
    logger_inst.setLevel(Setting.LEVEL)

    return logger_inst
