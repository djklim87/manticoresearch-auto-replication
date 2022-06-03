<?php

namespace Core\Notifications;

class NotificationStub implements NotificationInterface
{
    public function sendMessage($message): bool
    {
        echo "=> Notification: ".$message."\n";
        return false;
    }
}
