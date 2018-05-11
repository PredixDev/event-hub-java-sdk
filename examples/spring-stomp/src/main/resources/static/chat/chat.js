class Chat {
    constructor(container, classes, options) {
        this.classes = classes != null ? classes : this.defaultClasses();
        this.options = options != null ? options : this.defaultOptions();

        this.container = this.addClasses(container, [this.classes.container]);

        this.setupConversation();
    }


    setupConversation() {
        this.conversation = this.addClasses(document.createElement("div"), [this.classes.conversation]);
        this.container.appendChild(this.conversation);

        var top = document.createElement("div")
        top.style.height = this.options.conversationTopMargin;
        this.bottom = document.createElement("div")
        this.bottom.style.height = this.options.conversationBottomMargin;
        this.addClasses(this.bottom, ["bottom-last-chat-element"]);

        this.conversation.appendChild(top);
        this.conversation.appendChild(this.bottom);
    }
    
    addMessage(messageType, text) {
        var message = this.addClasses(document.createElement("div"), [this.classes.message, this.classes.messageTypes[messageType]]);
        var clearfix = this.addClasses(document.createElement("div"), [this.classes.clearfix]);

        this.conversation.insertBefore(message, this.bottom);
        this.conversation.insertBefore(clearfix, this.bottom);

        var that = this;
        setTimeout(function() {
            that.addClasses(message, [that.classes.messageShow]);
            message.innerHTML = text;            
            that.conversation.scrollTop = that.conversation.scrollHeight;                 
        },1);
    }

    clearMessages() {
        var messages = this.conversation.getElementsByClassName(this.classes.message);
        var clearfix = this.conversation.getElementsByClassName(this.classes.clearfix);
        
        while(messages.length > 0) {
            this.conversation.removeChild(messages[0]);
        }
        while(clearfix.length > 0) {
            this.conversation.removeChild(clearfix[0]);
        }
    }

    clearInput() {
        this.input.value = "";
    }

    addClasses(element, classList) {
        for(var i = 0; i < classList.length; i++) {
            element.classList.add(classList[i]);
        }
        return element;
    }

    defaultClasses() {
        return {
            container: "conversation-container",
            conversation: "conversation",
            input: "input",
            inputDropIn: "dropIn",
            clearfix: "clearfix",
            message: "msg",
            messageShow: "show",
            messageTypes: {
                info: "info",
                ack: "ack",
                subscribe: "subscribe",
                publish: "publish"
            }
        };
    } 
    defaultOptions() {
        return {
            conversationTopMargin: "10px",
            conversationBottomMargin: "85px",
            defaultMessageType: "publish",
            triggerChar: "/"
        };
    } 
}


