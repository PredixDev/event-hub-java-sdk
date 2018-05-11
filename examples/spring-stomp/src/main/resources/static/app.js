var chatInstance;
var stompClient = null;
var actions = null;

window.onload = function(){
    intro();
    setTimeout(function() {
        initChat();
        connect();
    }, 1500);


}

var intro = function(){
    var backdrop = document.getElementById("backdrop");
    var html = document.getElementsByTagName("html");
    setTimeout(function() {
        backdrop.classList.add("dropIn");
        setTimeout(function() {
            html[0].classList.add("fill");
        }, 1500);
    }, 500);
}

var initChat = function(){
    var chat = document.getElementById("conversation-container");
    chatInstance = new Chat(chat);
    actions = new Actions(chatInstance, chatInstance.options.triggerChar);
    setupInput();
    // Some fun things for now
    setTimeout(function() {
        chatInstance.addMessage("info","Welcome to the Predix Event Hub SDK Sample App");
        setTimeout(function() {
            chatInstance.addMessage("info","Messages will be sent to the server where the eventhub sdk will ingest them");
        } ,1000);
    } ,500);
}

var stompClient = null;

function setConnected(connected) {
    $("#connect").prop("disabled", connected);
    $("#disconnect").prop("disabled", !connected);
    if (connected) {
        $("#conversation").show();
    }
    else {
        $("#conversation").hide();
    }
    $("#greetings").html("");
}

function pubAckToText(pubAckJson){
    return "<h3>Received Ack</h3>id: " + pubAckJson.id + "<br>status: " + pubAckJson.status + "<br>topic: " + pubAckJson.topic + "<br>offset: " + pubAckJson.offset + "<br>partition: " + pubAckJson.partition;
}

function subscribeMessageToText(subscribeMessage){
        return "<h3>Received Message</h3>id: " + subscribeMessage.id + "<br>body: " + subscribeMessage.body + "<br>topic: " + subscribeMessage.topic + "<br>offset: " + subscribeMessage.offset + "<br>partition: " + subscribeMessage.partition + "<br>tags: " + JSON.stringify(subscribeMessage.tags);
}

function pubMessageToText(message){
    //message is just the text in the input box
    return "<h3>Published Message</h3>body: "+ message;
}

var onConnect = function(){
    chatInstance.addMessage("info", "<h3>Client Disconnected from Server</h3>")
}


var onDisconnect = function(){
    chatInstance.addMessage("info", "<h3>Client Disconnected From Server</h3>")
}



function connect() {
    var socket = new SockJS('/eventhub');
    stompClient = Stomp.over(socket);

    stompClient.connect({}, function (frame) {
        setConnected(true);
        console.log('Connected: ' + frame);
        stompClient.subscribe('/topic/publishAck', function (pubAck) {
            chatInstance.addMessage("ack", pubAckToText(JSON.parse(pubAck.body)));
        });
        stompClient.subscribe('/topic/subscribeMessage', function(subscribeMessage){
            chatInstance.addMessage("subscribe", subscribeMessageToText(JSON.parse(subscribeMessage.body)));
        });

        setInterval(function(){
            stompClient.send("/app/heartbeat", {} , JSON.stringify({"timestamp": Date.now()}));
        }, 5000);

    }, ()=> {
        onDisconnect();
    });
}

function disconnect() {
    if (stompClient !== null) {
        stompClient.disconnect();
    }
    setConnected(false);
    console.log("Disconnected");
}

function publishMessage(msg) {
    stompClient.send("/app/publishMessage", {}, JSON.stringify({'body': msg}));
}

function showGreeting(message) {
    $("#greetings").append("<tr><td>" + message + "</td></tr>");
}

function setupInput() {
    chatInstance.input = chatInstance.addClasses(document.createElement("input"), [chatInstance.classes.input]);
    chatInstance.container.appendChild(chatInstance.input);

    document.onkeydown = function(e) {
        chatInstance.input.focus(); // for now map everything to input
    };

    chatInstance.input.addEventListener("keypress", function(e){
        var key = e.which || e.keyCode;
        var value = chatInstance.input.value.replace(/^(\s)|(\s+)$/g, ""); // removes whitespace before and after
        if (key === 13 && value != "") {
            actions.parse(value);
        }
    });

    setTimeout(function() {
        chatInstance.addClasses(chatInstance.input, [chatInstance.classes.inputDropIn]);
        chatInstance.input.focus();
    },1);
}


class Actions {
    constructor(chat, triggerChar) {
        this.chat = chatInstance;
        this.triggerChar = triggerChar;
        this.actionMessageType = "ack";
    }

    parse(message) {
        if(message[0] != this.triggerChar) {

            this.chat.addMessage(this.chat.options.defaultMessageType, pubMessageToText(message));
            publishMessage(message);
            this.chat.clearInput();
            return;
        }

        var command = message.substring(1).split(" ")[0]
        if(this[command] == null) {
            this.chat.addMessage(this.actionMessageType, "Unknown command: " + command);
            this.chat.clearInput();
            return;
        }

        this[command]();
        this.chat.clearInput();
    }

    hello() {
        this.chat.addMessage(this.actionMessageType, "<u>hello!</u> <br>beep bop beep beep");
    }

    clear() {
        this.chat.clearMessages();
    }
}
