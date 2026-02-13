import sys
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import queue
import logging
import socket
import random

# Import the ChatClient class
from client import ChatClient
from client import get_local_ip

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_all_local_ips():
    """Get all IPv4 addresses of local network interfaces"""
    ips = set()
    hostname = socket.gethostname()

    try:
        ips.add(socket.gethostbyname(hostname))
    except:
        pass

    try:
        for info in socket.getaddrinfo(hostname, None):
            ip = info[4][0]
            if not ip.startswith("127.") and '.' in ip:
                ips.add(ip)
    except:
        pass

    return sorted(ips)


class ModernTheme:
    """Theme colors and styles for the application"""
    # Color scheme
    BG_DARK = "#2C3E50"  # Dark blue background
    BG_LIGHT = "#34495E"  # Lighter blue for panels
    BG_ULTRA_LIGHT = "#3D566E"  # Even lighter for inputs

    TEXT_LIGHT = "#ECF0F1"  # Almost white text
    TEXT_MUTED = "#BDC3C7"  # Muted gray text

    ACCENT = "#3498DB"  # Blue accent
    ACCENT_HOVER = "#2980B9"  # Darker blue for hover

    SUCCESS = "#2ECC71"  # Green
    WARNING = "#F39C12"  # Orange
    ERROR = "#E74C3C"  # Red

    # Font configurations
    FONT_FAMILY = "Segoe UI"
    FONT_SIZE_SMALL = 9
    FONT_SIZE_NORMAL = 10
    FONT_SIZE_LARGE = 12

    # Message colors by username (will be randomly assigned)
    USERNAME_COLORS = [
        "#3498DB",  # Blue
        "#2ECC71",  # Green
        "#9B59B6",  # Purple
        "#E67E22",  # Orange
        "#1ABC9C",  # Turquoise
        "#F1C40F",  # Yellow
        "#C0392B",  # Red
        "#16A085",  # Dark turquoise
    ]

    @staticmethod
    def get_random_color():
        """Return a random color from the username colors list"""
        return random.choice(ModernTheme.USERNAME_COLORS)


class ChatClientGUI:
    """
    GUI for the distributed chat client.
    """

    def __init__(self, root):
        """
        Initialize the chat client GUI.

        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("Multiplayer Chat")
        self.root.geometry("1000x700")
        self.root.minsize(800, 500)

        # Set dark theme
        self.root.configure(bg=ModernTheme.BG_DARK)

        # Configure styles
        self.style = ttk.Style()
        if 'clam' in self.style.theme_names():
            self.style.theme_use('clam')  # Use clam theme if available

        # Configure common styles
        self.style.configure("TFrame", background=ModernTheme.BG_DARK)
        self.style.configure("TLabel", background=ModernTheme.BG_DARK, foreground=ModernTheme.TEXT_LIGHT)
        self.style.configure("TButton", background=ModernTheme.ACCENT, foreground=ModernTheme.TEXT_LIGHT)

        # Configure Label Frame
        self.style.configure("TLabelframe", background=ModernTheme.BG_DARK)
        self.style.configure("TLabelframe.Label", background=ModernTheme.BG_DARK, foreground=ModernTheme.TEXT_LIGHT)

        # Username to color mapping
        self.username_colors = {}

        # Client instance
        self.client = None

        # Message display queue
        self.message_queue = queue.Queue()

        # Status variables
        self.connection_status = tk.StringVar(value="Disconnected")
        self.server_info = tk.StringVar(value="No server information")

        # Create and arrange widgets
        self._create_widgets()

        # Set up polling for new messages
        self.root.after(100, self._poll_messages)

        self.root.after(1000, lambda: self._add_chat_message("System",
                                                             "Welcome to Multiplayer Chat! Connect to a server to begin. Note: This chat room is for LAN Connection, enjoy!",
                                                             time.time()))

    def _update_status_display(self, status):
        """Update the status display with server information."""
        try:
            self.server_status_text.config(state="normal")
            self.server_status_text.delete(1.0, tk.END)
            self.server_status_text.insert(tk.END, f"Server: {status['server_id']} ({status['role']})\n")
            if status['leader_id']:
                self.server_status_text.insert(tk.END, f"Leader: {status['leader_id']}")
            self.server_status_text.config(state="disabled")
        except Exception as e:
            print(f"Error updating status display: {e}")

    def _show_local_ips(self):
        """Display all available IP addresses of the local machine"""
        ips = get_all_local_ips()
        msg = "All available IP addresses of the local machine (for LAN connection):\n\n" + "\n".join(ips)
        messagebox.showinfo("Local IP address", msg)

    def _create_widgets(self):
        """Create and layout GUI widgets."""
        # Main paned window to divide the GUI
        main_pane = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Left panel (connection info and controls)
        left_frame = ttk.Frame(main_pane, width=250)
        main_pane.add(left_frame, weight=1)

        # Right panel (chat area)
        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=3)

        # ---- Left panel contents ----
        left_frame.grid_columnconfigure(0, weight=1)

        # App title
        title_frame = ttk.Frame(left_frame)
        title_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=5)

        app_title = ttk.Label(title_frame, text="Multiplayer Chat",
                              font=(ModernTheme.FONT_FAMILY, 16, "bold"),
                              foreground=ModernTheme.ACCENT)
        app_title.pack(pady=10)

        app_subtitle = ttk.Label(title_frame, text="Distributed LAN Chat System",
                                 foreground=ModernTheme.TEXT_MUTED)
        app_subtitle.pack(pady=5)

        # Connection section
        conn_frame = ttk.LabelFrame(left_frame, text="Connection")
        conn_frame.grid(row=1, column=0, sticky="ew", padx=5, pady=5)

        ttk.Label(conn_frame, text="Server Address:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.server_entry = ttk.Entry(conn_frame)
        local_ip = get_local_ip()
        self.server_entry.insert(0, f"{local_ip}:50070")
        self.server_entry.grid(row=1, column=0, sticky="ew", padx=5, pady=2)

        ttk.Label(conn_frame, text="DNS Address (optional):").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        self.dns_entry = ttk.Entry(conn_frame)
        self.dns_entry.insert(0, f"{local_ip}:50051")
        self.dns_entry.grid(row=3, column=0, sticky="ew", padx=5, pady=2)

        button_frame = ttk.Frame(conn_frame)
        button_frame.grid(row=4, column=0, sticky="ew", padx=5, pady=5)
        button_frame.columnconfigure(0, weight=1)
        button_frame.columnconfigure(1, weight=1)

        self.connect_button = ttk.Button(button_frame, text="Connect", command=self._connect)
        self.connect_button.grid(row=0, column=0, sticky="w", padx=5, pady=5)

        self.ip_button = ttk.Button(button_frame, text="View IPs", command=self._show_local_ips)
        self.ip_button.grid(row=0, column=1, sticky="e", padx=5, pady=5)

        # User info section
        user_frame = ttk.LabelFrame(left_frame, text="User Information")
        user_frame.grid(row=2, column=0, sticky="ew", padx=5, pady=5)

        ttk.Label(user_frame, text="Username:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.username_entry = ttk.Entry(user_frame)
        self.username_entry.insert(0, f"User-{int(time.time()) % 1000}")
        self.username_entry.grid(row=1, column=0, sticky="ew", padx=5, pady=2)

        ttk.Label(user_frame, text="Room:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        self.room_entry = ttk.Entry(user_frame)
        self.room_entry.insert(0, "default")
        self.room_entry.grid(row=3, column=0, sticky="ew", padx=5, pady=2)

        self.login_button = ttk.Button(user_frame, text="Join Chat", command=self._join_chat)
        self.login_button.grid(row=4, column=0, sticky="ew", padx=5, pady=5)
        self.login_button["state"] = "disabled"

        # Status section
        status_frame = ttk.LabelFrame(left_frame, text="Status")
        status_frame.grid(row=3, column=0, sticky="ew", padx=5, pady=5)

        status_indicator_frame = ttk.Frame(status_frame)
        status_indicator_frame.pack(fill="x", padx=5, pady=2)

        # Simple status indicator (color square)
        self.status_indicator = tk.Label(status_indicator_frame, text="⬤",
                                         font=(ModernTheme.FONT_FAMILY, 12),
                                         foreground=ModernTheme.ERROR,
                                         background=ModernTheme.BG_DARK)
        self.status_indicator.pack(side="left", padx=5)

        ttk.Label(status_indicator_frame, textvariable=self.connection_status).pack(side="left", padx=5)

        self.server_status_text = tk.Text(status_frame, wrap=tk.WORD, height=6,
                                          bg=ModernTheme.BG_ULTRA_LIGHT, fg=ModernTheme.TEXT_LIGHT,
                                          highlightthickness=0)
        self.server_status_text.pack(fill="x", padx=5, pady=5)
        self.server_status_text.config(state="disabled")

        self.status_button = ttk.Button(status_frame, text="Get Status", command=self._get_status)
        self.status_button.pack(fill="x", padx=5, pady=5)
        self.status_button["state"] = "disabled"

        # ---- Right panel contents ----
        right_frame.grid_columnconfigure(0, weight=1)
        right_frame.grid_rowconfigure(0, weight=1)

        # Chat display area
        chat_frame = ttk.LabelFrame(right_frame, text="Chat Messages")
        chat_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        chat_frame.grid_columnconfigure(0, weight=1)
        chat_frame.grid_rowconfigure(0, weight=1)

        self.chat_display = scrolledtext.ScrolledText(chat_frame, wrap=tk.WORD,
                                                      bg=ModernTheme.BG_LIGHT,
                                                      fg=ModernTheme.TEXT_LIGHT,
                                                      highlightthickness=0,
                                                      font=(ModernTheme.FONT_FAMILY, 12)
                                                      )
        self.chat_display.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.chat_display.config(state="normal")

        # Configure tags for different message types
        self.chat_display.tag_configure("system", foreground=ModernTheme.WARNING)
        for i, color in enumerate(ModernTheme.USERNAME_COLORS):
            self.chat_display.tag_configure(f"user_{i}", foreground=color)

        self.chat_display.config(state="disabled")

        # Message input area
        input_frame = ttk.Frame(right_frame)
        input_frame.grid(row=1, column=0, sticky="ew", padx=5, pady=5)
        input_frame.grid_columnconfigure(0, weight=1)

        self.message_entry = ttk.Entry(input_frame)
        self.message_entry.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        self.message_entry.bind("<Return>", lambda e: self._send_message())
        self.message_entry["state"] = "disabled"

        self.send_button = ttk.Button(input_frame, text="Send", command=self._send_message)
        self.send_button.grid(row=0, column=1, padx=5, pady=5)
        self.send_button["state"] = "disabled"

    def _update_status_indicator(self, status):
        """Update the status indicator color"""
        if status == "connected":
            self.status_indicator.config(foreground=ModernTheme.SUCCESS)
        elif status == "connecting":
            self.status_indicator.config(foreground=ModernTheme.WARNING)
        else:
            self.status_indicator.config(foreground=ModernTheme.ERROR)

    def _connect(self):
        """Connect to the chat server."""
        # Show connecting status
        self.connection_status.set("Connecting...")
        self._update_status_indicator("connecting")
        self.root.update()  # Force UI update

        server_address = self.server_entry.get()
        dns_address = self.dns_entry.get() if self.dns_entry.get() else None

        try:
            # Create client instance
            self.client = ChatClient(server_address, dns_address)

            # Important: Ensure the client and GUI use the same message queue
            self.client.message_queue = self.message_queue
            print(f"Set client message queue to: {id(self.message_queue)}")

            # Connect to server
            if not self.client.connect():
                self._update_status_indicator("disconnected")
                self.connection_status.set("Disconnected")
                messagebox.showerror("Connection Failed", "Could not connect to the server.")
                return

            # Update UI
            self.connection_status.set("Connected")
            self._update_status_indicator("connected")
            self.connect_button["state"] = "disabled"
            self.login_button["state"] = "normal"

            messagebox.showinfo("Connection Successful", f"Connected to {server_address}")

        except Exception as e:
            self._update_status_indicator("disconnected")
            self.connection_status.set("Disconnected")
            messagebox.showerror("Connection Error", f"Error connecting to server: {e}")
            logger.error(f"Connection error: {e}")

    def _join_chat(self):
        """Join a chat room with improved leader failure handling."""
        if not self.client:
            messagebox.showerror("Error", "Not connected to a server.")
            return

        # Show connecting status
        self.connection_status.set("Joining room...")
        self._update_status_indicator("connecting")
        self.root.update()  # Force UI update

        username = self.username_entry.get()
        room_id = self.room_entry.get()

        try:
            # Login with username
            self.client.login(username)

            # Try to join the chat room with enhanced leader failure handling
            max_attempts = 10  # Increase total attempts
            attempt = 1

            while attempt <= max_attempts:
                try:
                    if self.client.join_chat(room_id):
                        # Successfully joined
                        self._setup_message_handler()
                        print(
                            f"Joined chat: client queue ID: {id(self.client.message_queue)}, GUI queue ID: {id(self.message_queue)}")
                        break
                    else:
                        # Failed, wait and retry
                        time.sleep(1)
                        attempt += 1
                        continue

                except Exception as e:
                    err_str = str(e)

                    # If the message contains redirection information, try to reconnect
                    if "Redirect to" in err_str:
                        # Extract new server address
                        parts = err_str.split("Redirect to ")
                        if len(parts) > 1:
                            new_address = parts[1].strip()
                            messagebox.showinfo("Redirecting", f"Redirecting to leader at {new_address}")

                            # Update server address
                            self.server_entry.delete(0, tk.END)
                            self.server_entry.insert(0, new_address)

                            # Reconnect
                            self.client.server_address = new_address
                            if self.client.connect():
                                continue

                    # Leader unavailable, try to find a new leader via DNS
                    elif "Server leader not found" in err_str or "No leader known" in err_str:
                        if self.client.dns_address:
                            new_leader = self._find_new_leader()
                            if new_leader:
                                messagebox.showinfo("Leader Changed", f"Discovered new leader at {new_leader}")

                                # Update server address
                                self.server_entry.delete(0, tk.END)
                                self.server_entry.insert(0, new_leader)

                                # Reconnect
                                self.client.server_address = new_leader
                                if self.client.connect():
                                    continue

                        # If leader cannot be found, try connecting to other known servers
                        self._try_alternate_servers()

                    # Other errors or redirection failed, increase retry count
                    attempt += 1
                    time.sleep(1)

            if attempt > max_attempts:
                self.connection_status.set("Connected")  # Revert status
                self._update_status_indicator("connected")
                messagebox.showerror("Join Failed", "Failed to join chat after multiple attempts.")
                return

            # Update UI
            self.connection_status.set(f"Chatting in '{room_id}'")
            self._update_status_indicator("connected")

            # Disable login controls
            self.login_button["state"] = "disabled"

            # Enable message controls
            self.message_entry["state"] = "normal"
            self.send_button["state"] = "normal"

            # Enable status button
            self.status_button["state"] = "normal"

            # Clear chat display
            self.chat_display.config(state="normal")
            self.chat_display.delete(1.0, tk.END)
            self.chat_display.config(state="disabled")

            # Add system message
            self._add_chat_message("System", f"Welcome to room: {room_id}", time.time())

            # Setup message handler
            self._setup_message_handler()

            messagebox.showinfo("Join Successful", f"Joined chat room: {room_id}")

        except Exception as e:
            self.connection_status.set("Connected")  # Revert status
            self._update_status_indicator("connected")
            messagebox.showerror("Join Error", f"Error joining chat: {e}")
            logger.error(f"Join error: {e}")

    def _find_new_leader(self):
        """Try to find a new leader from DNS or known server list"""
        try:
            # If DNS address is available, use DNS lookup
            if hasattr(self.client, 'dns_client') and self.client.dns_client:
                leader = self.client._find_leader_server()
                if leader:
                    return leader

            # If DNS lookup fails, try direct query to known servers
            return self._query_servers_for_leader()

        except Exception as e:
            logger.error(f"Error finding new leader: {e}")
            return None

    def _query_servers_for_leader(self):
        """Poll all possible server ports to find the leader"""
        base_addr = self.client.server_address.split(':')[0]

        # Try common server ports
        ports = [50050, 50060, 50070, 50080, 50090]

        for port in ports:
            try:
                # Create temporary client to test connection
                test_address = f"{base_addr}:{port}"

                # Avoid trying already failed address
                if test_address == self.client.server_address:
                    continue

                # Try connecting
                temp_client = ChatClient(test_address, self.client.dns_address)
                if temp_client.connect():
                    # Try to get server status
                    status = temp_client.get_server_status()
                    if status and status.get('role') == 'LEADER':
                        temp_client.disconnect()
                        return test_address
                    temp_client.disconnect()
            except:
                pass

        return None

    def _try_alternate_servers(self):
        """Try connecting to other servers"""
        base_addr = self.client.server_address.split(':')[0]
        current_port = int(self.client.server_address.split(':')[1])

        # List of ports to try, prioritize other known ports
        ports = [50050, 50060, 50070, 50080, 50090]
        ports = [p for p in ports if p != current_port]

        for port in ports:
            try:
                new_address = f"{base_addr}:{port}"
                logger.info(f"Trying alternate server: {new_address}")

                # Update client address
                self.client.server_address = new_address

                # Try connecting
                if self.client.connect():
                    # Update UI display
                    self.server_entry.delete(0, tk.END)
                    self.server_entry.insert(0, new_address)
                    return True
            except Exception as e:
                logger.error(f"Failed to connect to alternate server {new_address}: {e}")

        return False

    def _handle_connection_lost(self):
        """Handle connection loss situation"""
        if not hasattr(self, '_reconnecting') or not self._reconnecting:
            self._reconnecting = True

            # Update UI status
            self.connection_status.set("Reconnecting...")
            self._update_status_indicator("connecting")

            # Try reconnecting in a new thread to avoid blocking UI
            threading.Thread(target=self._attempt_reconnect, daemon=True).start()

    def _attempt_reconnect(self):
        """Attempt to reconnect to the server"""
        try:
            # First try to find a new leader
            new_leader = self._find_new_leader()
            if new_leader:
                # Update server address
                self.client.server_address = new_leader
                self.root.after(0, lambda: self.server_entry.delete(0, tk.END))
                self.root.after(0, lambda: self.server_entry.insert(0, new_leader))

            # Try reconnecting
            if self.client.reconnect():
                # Rejoin chat room
                if self.client.join_chat(self.room_entry.get()):
                    # Successfully reconnected
                    self.root.after(0, lambda: self.connection_status.set("Connected"))
                    self.root.after(0, lambda: self._update_status_indicator("connected"))
                    self.root.after(0, lambda: self._add_chat_message("System", "Reconnected to server", time.time()))
                    self._reconnecting = False
                    return

            # If direct reconnection fails, try polling other servers
            if self._try_alternate_servers():
                # Try rejoining the chat room
                if self.client.join_chat(self.room_entry.get()):
                    self.root.after(0, lambda: self.connection_status.set("Connected"))
                    self.root.after(0, lambda: self._update_status_indicator("connected"))
                    self.root.after(0, lambda: self._add_chat_message("System", "Connected to alternate server",
                                                                      time.time()))
                    self._reconnecting = False
                    return

            # All attempts failed
            self.root.after(0, lambda: self.connection_status.set("Disconnected"))
            self.root.after(0, lambda: self._update_status_indicator("disconnected"))
            self.root.after(0, lambda: messagebox.showerror("Connection Lost",
                                                            "Cannot reconnect to any server. Please check if servers are running."))

        except Exception as e:
            logger.error(f"Error in reconnection attempt: {e}")

        finally:
            self._reconnecting = False

    def _setup_message_handler(self):
        """Set up message handler callback to receive and display messages."""
        if self.client:
            # Ensure client uses GUI message queue
            self.client.message_queue = self.message_queue

            # Set direct callback function
            def display_callback(message):
                # Safely update GUI in the main thread
                self.root.after(0, lambda: self._add_chat_message(
                    message['username'],
                    message['content'],
                    message['timestamp']
                ))

            def status_callback(status):
                # Safely update status display in the main thread
                self.root.after(0, lambda: self._update_status_display(status))

            self.client.display_callback = display_callback
            self.client.status_callback = status_callback
            print(f"Message handler setup complete, using queue: {id(self.message_queue)}")

    def _send_message(self):
        """Send a chat message and display it locally."""
        if not self.client:
            return

        message = self.message_entry.get()
        if not message.strip():
            return

        try:
            # Send message to server
            success = self.client.send_message(message)

            # Clear message input box
            self.message_entry.delete(0, tk.END)

        except Exception as e:
            messagebox.showerror("Send Error", f"Error sending message: {e}")
            logger.error(f"Send error: {e}")

    def _get_status(self):
        """Get server status information."""
        if not self.client:
            messagebox.showerror("Error", "Not connected to a server.")
            return

        try:
            status = self.client.get_server_status()

            if status:
                # Update status display
                self.server_status_text.config(state="normal")
                self.server_status_text.delete(1.0, tk.END)

                status_text = f"Server: {status['server_id']} ({status['role']})\n"
                status_text += f"Leader: {status['leader_id']}\n"
                status_text += f"Term: {status['term']}\n"
                status_text += f"Peers: {', '.join(status['peer_servers'])}"

                self.server_status_text.insert(tk.END, status_text)
                self.server_status_text.config(state="disabled")
            else:
                messagebox.showerror("Status Error", "Failed to get server status.")

        except Exception as e:
            messagebox.showerror("Status Error", f"Error getting server status: {e}")
            logger.error(f"Status error: {e}")

    def _get_username_tag(self, username):
        """Get a tag for a username, creating it if needed"""
        if username == "System":
            return "system"

        if username not in self.username_colors:
            tag_index = len(self.username_colors) % len(ModernTheme.USERNAME_COLORS)
            self.username_colors[username] = f"user_{tag_index}"

        return self.username_colors[username]

    def _add_chat_message(self, username, content, timestamp):
        """Add a message to the chat display."""
        try:
            # Ensure text box is editable
            self.chat_display.config(state="normal")

            # Format timestamp
            time_str = time.strftime("%H:%M:%S", time.localtime(timestamp))

            # Get tag for username
            username_tag = self._get_username_tag(username)

            # Insert timestamp and username with appropriate color
            self.chat_display.insert(tk.END, f"[{time_str}] ", "timestamp")
            self.chat_display.insert(tk.END, f"{username}: ", username_tag)

            # Insert message content
            self.chat_display.insert(tk.END, f"{content}\n")

            # Scroll to bottom
            self.chat_display.see(tk.END)

            # Set back to read-only
            self.chat_display.config(state="disabled")

        except Exception as e:
            print(f"Error displaying message: {e}")

    def _poll_messages(self):
        """Poll for new messages from the client's message queue."""
        try:
            # Process all messages in the queue
            if self.client and hasattr(self.client, 'message_queue'):
                try:
                    # Directly check client's message queue
                    client_queue = self.client.message_queue

                    # Process all available messages
                    while not client_queue.empty():
                        try:
                            message = client_queue.get_nowait()
                            print(f"Processing message: {message}")  # Debug output

                            if 'type' in message and message['type'] == 'chat':
                                # Add to chat display
                                self._add_chat_message(
                                    message['username'],
                                    message['content'],
                                    message['timestamp']
                                )

                            # Mark task as done
                            client_queue.task_done()
                        except Exception as e:
                            print(f"Error processing individual message: {e}")
                except Exception as e:
                    print(f"Error accessing client message queue: {e}")
        except Exception as e:
            print(f"Error in message polling main loop: {e}")

        # Schedule next poll
        self.root.after(100, self._poll_messages)

    def on_closing(self):
        """Handle window closing event."""
        if self.client:
            try:
                self.client.disconnect()
            except:
                pass

        self.root.destroy()


def main():
    """Main entry point for the chat client GUI."""
    root = tk.Tk()
    app = ChatClientGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()