# from wechatpy.work.crypto import WeChatCrypto
# from wechatpy.exceptions import InvalidSignatureException
# from wechatpy.work.exceptions import InvalidCorpIdException
# from wechatpy.work import parse_message, create_reply
import os
from wechatpy.enterprise import WeChatClient
wechat_client = WeChatClient(CORP_ID, SECRET)

CORP_ID = 'ww01fac26127e4ee29'
AGENT_ID = '1000002'
SECRET = 'Dog55Mun0E9vWjG8fyMCVQWvoyxy-VHDoF8TdaXYz2g'

def notify(title, text):
    os.system("""
              osascript -e 'display notification "{}" with title "{}"'
              """.format(text, title))


# user = client.user.get('ZhuTuo')
# print(user)

if __name__ == '__main__':
    # test wechat notification
    ret = wechat_client.message.send_text(AGENT_ID, 'ZhuTuo', 'alla')
    if ret['errcode'] == 0:
        print("Successfully sent wechat notification!")

    # test macos sys notification
    notify("Title", "Heres an alert")

