// Chat functionality for Shakespeare chatbot
let isProcessing = false;

function initializeChat() {
    const chatInput = document.getElementById('chat-input');
    const chatSend = document.getElementById('chat-send');
    const chatClear = document.getElementById('chat-clear');
    const chatMessages = document.getElementById('chat-messages');
    
    if (!chatInput || !chatSend || !chatMessages) return;
    
    // Send message function
    async function sendMessage() {
        const message = chatInput.value.trim();
        if (!message || isProcessing) return;
        
        isProcessing = true;
        chatSend.disabled = true;
        chatInput.disabled = true;
        
        // Add user message to chat
        addMessage(message, 'user');
        chatInput.value = '';
        
        // Show typing indicator
        const typingId = showTypingIndicator();
        
        try {
            // Use streaming for better UX
            const response = await fetch('/api/chat/stream', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ message: message })
            });
            
            if (!response.ok) {
                throw new Error('Failed to get response');
            }
            
            // Remove typing indicator
            removeTypingIndicator(typingId);
            
            // Create assistant message element
            const assistantMessage = addMessage('', 'assistant', true);
            const messageContent = assistantMessage.querySelector('.message-content');
            
            // Read the stream
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let fullResponse = '';
            
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                
                const chunk = decoder.decode(value);
                const lines = chunk.split('\n');
                
                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.slice(6));
                            if (data.chunk) {
                                fullResponse += data.chunk;
                                messageContent.textContent = fullResponse;
                                scrollToBottom();
                            }
                        } catch (e) {
                            // Ignore parse errors
                        }
                    }
                }
            }
            
            // Remove streaming class when done
            messageContent.classList.remove('streaming');
            
        } catch (error) {
            console.error('Error:', error);
            removeTypingIndicator(typingId);
            addMessage('Sorry, I encountered an error. Please try again.', 'assistant');
        } finally {
            isProcessing = false;
            chatSend.disabled = false;
            chatInput.disabled = false;
            chatInput.focus();
        }
    }
    
    // Add message to chat
    function addMessage(content, role, streaming = false) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `chat-message ${role}`;
        
        const roleDiv = document.createElement('div');
        roleDiv.className = 'message-role';
        roleDiv.textContent = role === 'user' ? 'You' : 'Shakespeare Assistant';
        
        const contentDiv = document.createElement('div');
        contentDiv.className = 'message-content' + (streaming ? ' streaming' : '');
        contentDiv.textContent = content;
        
        messageDiv.appendChild(roleDiv);
        messageDiv.appendChild(contentDiv);
        
        chatMessages.appendChild(messageDiv);
        scrollToBottom();
        
        return messageDiv;
    }
    
    // Show typing indicator
    function showTypingIndicator() {
        const indicatorDiv = document.createElement('div');
        indicatorDiv.className = 'chat-message assistant';
        indicatorDiv.id = `typing-${Date.now()}`;
        
        const roleDiv = document.createElement('div');
        roleDiv.className = 'message-role';
        roleDiv.textContent = 'Shakespeare Assistant';
        
        const typingDiv = document.createElement('div');
        typingDiv.className = 'typing-indicator';
        typingDiv.innerHTML = '<span></span><span></span><span></span>';
        
        indicatorDiv.appendChild(roleDiv);
        indicatorDiv.appendChild(typingDiv);
        
        chatMessages.appendChild(indicatorDiv);
        scrollToBottom();
        
        return indicatorDiv.id;
    }
    
    // Remove typing indicator
    function removeTypingIndicator(id) {
        const indicator = document.getElementById(id);
        if (indicator) {
            indicator.remove();
        }
    }
    
    // Scroll to bottom of chat
    function scrollToBottom() {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
    
    // Clear chat
    async function clearChat() {
        if (!confirm('Are you sure you want to clear the conversation?')) return;
        
        try {
            await fetch('/api/chat/clear', {
                method: 'POST'
            });
            
            // Clear messages except welcome
            const messages = chatMessages.querySelectorAll('.chat-message');
            messages.forEach((msg, index) => {
                if (index > 0) msg.remove();
            });
            
        } catch (error) {
            console.error('Error clearing chat:', error);
        }
    }
    
    // Event listeners
    chatSend.addEventListener('click', sendMessage);
    
    chatInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });
    
    chatClear.addEventListener('click', clearChat);
    
    // Focus input on load
    chatInput.focus();
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeChat);
} else {
    initializeChat();
}