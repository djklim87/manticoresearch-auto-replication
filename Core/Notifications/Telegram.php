<?php

namespace Core\Notifications;

class Telegram implements NotificationInterface
{
    private $chatId;
    private $token;

    public function __construct($chatID, $token)
    {
        $this->chatId = $chatID;
        $this->token  = $token;
    }

    public function sendMessage($message): bool
    {
        echo "=> Notification: ".$message."\n";
        $url      = "https://api.telegram.org/bot".$this->token."/sendMessage?chat_id=".$this->chatId;
        $url      .= "&text=".urlencode($message);
        $ch       = curl_init();
        $optArray = [
            CURLOPT_URL            => $url,
            CURLOPT_RETURNTRANSFER => true,
        ];
        curl_setopt_array($ch, $optArray);
        $result = curl_exec($ch);
        curl_close($ch);

        return $result;
    }
}
