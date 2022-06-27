# from wechatpy.work.crypto import WeChatCrypto
# from wechatpy.exceptions import InvalidSignatureException
# from wechatpy.work.exceptions import InvalidCorpIdException
# from wechatpy.work import parse_message, create_reply
import os
from wechatpy.enterprise import WeChatClient

import sys
sys.path.append('./')
from utils.logger import logger

CORP_ID = 'ww01fac26127e4ee29'
SHAW_AGENT_ID = '1000002'
LANI_AGENT_ID = '1000003'
SHAW_SECRET = 'Dog55Mun0E9vWjG8fyMCVQWvoyxy-VHDoF8TdaXYz2g'
LANI_SECRET = 'CLJ3739fd3r2vg14Kt-0F-7zDKj84BmiBUULf7WfVl0'

Bots = {
        'shaw': {
            'id': SHAW_AGENT_ID,
            'secret': SHAW_SECRET,
            'bot': WeChatClient(CORP_ID, SHAW_SECRET)
            },
        'lani': {
            'id': LANI_AGENT_ID,
            'secret': LANI_SECRET,
            'bot': WeChatClient(CORP_ID, LANI_SECRET)
            },
        }


def notify(title, text):
    os.system("""
              osascript -e 'display notification "{}" with title "{}"'
              """.format(text, title))


class WechatBot():

    def __init__(self, users='ZhuTuo', bot_name='lani'):
        self.bot = Bots[bot_name]['bot']
        self.agent_id = Bots[bot_name]['id']
        self.users = users


    def send_image(self, file_path):
        with open(file_path, 'rb') as f:
            media_file = f.read()
            ret = self.bot.media.upload(media_file=media_file, media_type='image')
            image_id = ret['media_id']
            self.bot.message.send_image(agent_id=self.agent_id, user_ids=self.users, media_id=image_id)


    def send_text(self, text):
        ret = self.bot.message.send_text(self.agent_id, self.users, text)
        if ret['errcode'] != 0:
            logger.error(ret)


wechat_bot = WechatBot()
# user = client.user.get('ZhuTuo')
# print(user)

if __name__ == '__main__':
    # test wechat notification
    ret = wechat_client.message.send_text(AGENT_ID, 'ZhuTuo', 'alla')
    if ret['errcode'] == 0:
        print("Successfully sent wechat notification!")

    # test macos sys notification
    # notify("Title", "Heres an alert")

