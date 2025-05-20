import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import random
import time
import asyncio
from datetime import timedelta
from contextlib import closing


class EconomyCog(commands.Cog):
    DEFAULT_SHOP_ITEMS = {
        "coffee": {"price": 500, "description": "☕ Energy boost (XP bonus for 1h)", "emoji": "☕", "duration": 3600},
        "laptop": {"price": 1000, "description": "💻 Work bonus (higher work income for 24h)", "emoji": "💻", "duration": 86400},
        "dice": {"price": 2000, "description": "🎲 Lucky dice (better gambling odds for 1h)", "emoji": "🎲", "duration": 3600},
        "shield": {"price": 2500, "description": "🛡️ Theft protection (24h)", "emoji": "🛡️", "duration": 86400},
        "bankrob": {"price": 8000, "description": "💰 Bank robbery kit (steal from bank for 24h)", "emoji": "💰", "duration": 86400}  # Fixed duration
    }

    def __init__(self, bot):
        self.bot = bot
        self.init_db()
        self.migrate_database()
        self.ensure_default_shop_items()

    def get_db_connection(self):
        """Secure database connection with enhanced settings"""
        conn = sqlite3.connect('economy.db', timeout=30.0, isolation_level='IMMEDIATE')
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Initialize database structure with proper defaults"""
        with closing(self.get_db_connection()) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                cash INTEGER DEFAULT 100 NOT NULL,
                bank INTEGER DEFAULT 0 NOT NULL,
                xp INTEGER DEFAULT 0 NOT NULL,
                level INTEGER DEFAULT 1 NOT NULL,
                daily_cooldown INTEGER,
                work_cooldown INTEGER,
                stream_cooldown INTEGER,
                steal_cooldown INTEGER,
                total_earned INTEGER DEFAULT 0 NOT NULL,
                total_spent INTEGER DEFAULT 0 NOT NULL,
                games_played INTEGER DEFAULT 0 NOT NULL,
                games_won INTEGER DEFAULT 0 NOT NULL
            )''')

            conn.execute('''CREATE TABLE IF NOT EXISTS shop_items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                price INTEGER NOT NULL,
                description TEXT NOT NULL,
                emoji TEXT NOT NULL,
                UNIQUE(guild_id, item_name)
            )''')

            conn.execute('''CREATE TABLE IF NOT EXISTS user_inventory (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER DEFAULT 1 NOT NULL,
                expires_at INTEGER,
                used_at INTEGER,
                PRIMARY KEY (user_id, item_id),
                FOREIGN KEY (item_id) REFERENCES shop_items(item_id)
            )''')

            conn.execute('''CREATE TABLE IF NOT EXISTS used_items (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                emoji TEXT,
                used_at INTEGER NOT NULL,
                expires_at INTEGER
            )''')

            try:
                conn.execute("ALTER TABLE user_inventory ADD COLUMN used_at INTEGER")
            except sqlite3.OperationalError:
                pass

            conn.commit()

    def migrate_database(self):
        """Migrate existing databases to new format"""
        try:
            with closing(self.get_db_connection()) as conn:
                # Migration for users table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS users_new AS
                    SELECT 
                        user_id, username, 
                        COALESCE(cash, 100) as cash,
                        COALESCE(bank, 0) as bank,
                        COALESCE(xp, 0) as xp,
                        COALESCE(level, 1) as level,
                        CAST(daily_cooldown AS INTEGER) as daily_cooldown,
                        CAST(work_cooldown AS INTEGER) as work_cooldown,
                        CAST(stream_cooldown AS INTEGER) as stream_cooldown,
                        CAST(steal_cooldown AS INTEGER) as steal_cooldown,
                        COALESCE(total_earned, 0) as total_earned,
                        COALESCE(total_spent, 0) as total_spent,
                        COALESCE(games_played, 0) as games_played,
                        COALESCE(games_won, 0) as games_won
                    FROM users
                """)
                conn.execute("DROP TABLE IF EXISTS users")
                conn.execute("ALTER TABLE users_new RENAME TO users")
                conn.commit()
        except Exception as e:
            print(f"Migration failed: {e}")

    def ensure_default_shop_items(self):
        try:
            with closing(self.get_db_connection()) as conn:
                # Lösche zuerst doppelte Einträge
                conn.execute("""
                    DELETE FROM shop_items 
                    WHERE item_id NOT IN (
                        SELECT MIN(item_id) 
                        FROM shop_items 
                        GROUP BY LOWER(item_name), guild_id
                    )
                """)

                # Füge Standard-Items hinzu
                for guild in self.bot.guilds:
                    for item_name, item_data in self.DEFAULT_SHOP_ITEMS.items():
                        conn.execute("""
                            INSERT OR IGNORE INTO shop_items 
                            (guild_id, item_name, price, description, emoji)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            guild.id,
                            item_name,
                            item_data['price'],
                            item_data['description'],
                            item_data['emoji']
                        ))
                conn.commit()
        except Exception as e:
            print(f"Error ensuring shop items: {e}")

    def get_active_item(self, user_id, item_name):
        """Get active item details"""
        with closing(self.get_db_connection()) as conn:
            current_time = int(time.time())
            item = conn.execute("""
                SELECT ui.*, si.* FROM user_inventory ui
                JOIN shop_items si ON ui.item_id = si.item_id
                WHERE ui.user_id = ? AND LOWER(si.item_name) = LOWER(?) 
                AND (ui.expires_at IS NULL OR ui.expires_at > ?)
                AND ui.quantity > 0
            """, (user_id, item_name, current_time)).fetchone()
            return dict(item) if item else None

    async def use_item(self, user_id, item_name):
        """Verbesserte Item-Nutzung mit Fehlerbehebung"""
        with closing(self.get_db_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE TRANSACTION")
                current_time = int(time.time())
                item = conn.execute("""
                    SELECT ui.rowid, ui.quantity, si.item_id, si.item_name, ui.used_at
                    FROM user_inventory ui
                    JOIN shop_items si ON ui.item_id = si.item_id
                    WHERE ui.user_id = ? AND LOWER(si.item_name) = LOWER(?)
                    AND ui.quantity > 0
                """, (user_id, item_name)).fetchone()
                if not item or (item_name.lower() == "bankrob" and item['used_at'] is not None):
                    conn.rollback()
                    return False
                item_data = self.DEFAULT_SHOP_ITEMS.get(item['item_name'].lower(), {})
                print("item_data:", item_data, "type:", type(item_data))
                duration = item_data.get('duration', 0)  # <-- Hier muss .get statt ()
                expires_at = current_time + duration if duration else None
                if item_name.lower() == "bankrob":
                    conn.execute("""
                        UPDATE user_inventory
                        SET used_at = ?, expires_at = ?
                        WHERE rowid = ?
                    """, (current_time, expires_at, item['rowid']))
                    conn.execute("""
                        INSERT INTO used_items
                        (user_id, item_id, item_name, emoji, used_at, expires_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        user_id, item['item_id'], item['item_name'], item_data.get('emoji'), current_time, expires_at))
                else:
                    new_quantity = item['quantity'] - 1
                    if new_quantity <= 0:
                        conn.execute("DELETE FROM user_inventory WHERE rowid = ?", (item['rowid'],))
                    else:
                        conn.execute("""
                            UPDATE user_inventory
                            SET quantity = ?, used_at = ?, expires_at = ?
                            WHERE rowid = ?
                        """, (new_quantity, current_time, expires_at, item['rowid']))
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                print(f"Error in use_item command: {e}")  # Für Debugging
                return False

    def has_active_item(self, user_id, item_name):
        """Check if user has an active item with transaction safety"""
        with closing(self.get_db_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE TRANSACTION")
                current_time = int(time.time())
                if item_name.lower() == "bankrob":
                    item = conn.execute("""
                        SELECT 1 FROM user_inventory ui
                        JOIN shop_items si ON ui.item_id = si.item_id
                        WHERE ui.user_id = ? AND LOWER(si.item_name) = LOWER(?)
                        AND ui.quantity > 0 AND (ui.used_at IS NULL)
                    """, (user_id, item_name)).fetchone()
                else:
                    item = conn.execute("""
                        SELECT 1 FROM user_inventory ui
                        JOIN shop_items si ON ui.item_id = si.item_id
                        WHERE ui.user_id = ? AND LOWER(si.item_name) = LOWER(?)
                        AND (ui.expires_at IS NULL OR ui.expires_at > ?)
                        AND ui.quantity > 0
                    """, (user_id, item_name, current_time)).fetchone()
                conn.commit()
                return item is not None
            except:
                conn.rollback()
                raise

    def format_time_remaining(self, seconds: int) -> str:
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if sec > 0 or not parts:
            parts.append(f"{sec}s")

        return " ".join(parts)

    async def add_item_to_inventory(self, user_id, item_id, duration=None):
        """Add item to user's inventory with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with closing(self.get_db_connection()) as conn:
                    current_time = int(time.time())
                    expires_at = current_time + duration if duration else None
                    # Check if item already exists
                    existing = conn.execute("""
                        SELECT rowid, quantity FROM user_inventory
                        WHERE user_id = ? AND item_id = ?
                    """, (user_id, item_id)).fetchone()
                    # Für bankrob: Nur hinzufügen, wenn nicht vorhanden oder bereits benutzt
                    if self.DEFAULT_SHOP_ITEMS.get('bankrob', {}).get('item_id') == item_id:
                        bankrob = conn.execute("""
                            SELECT * FROM user_inventory
                            WHERE user_id = ? AND item_id = ? AND used_at IS NULL
                        """, (user_id, item_id)).fetchone()
                        if bankrob:
                            return False
                    if existing:
                        # Update existing entry
                        conn.execute("""
                            UPDATE user_inventory
                            SET quantity = quantity + 1,
                                expires_at = COALESCE(?, expires_at)
                            WHERE rowid = ?
                        """, (expires_at, existing['rowid']))
                    else:
                        # Insert new entry
                        conn.execute("""
                            INSERT INTO user_inventory
                            (user_id, item_id, quantity, expires_at)
                            VALUES (?, ?, 1, ?)
                        """, (user_id, item_id, expires_at))
                    conn.commit()
                    return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise
            except Exception as e:
                print(f"Error adding item to inventory: {e}")
                return False

    # Füge diese Methode zur EconomyCog Klasse hinzu
    async def cleanup_shop_items(self):
        """Bereinigt doppelte Shop-Einträge"""
        try:
            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    DELETE FROM shop_items 
                    WHERE item_id NOT IN (
                        SELECT MIN(item_id) 
                        FROM shop_items 
                        GROUP BY LOWER(item_name), guild_id
                    )
                """)
                conn.commit()
        except Exception as e:
            print(f"Error cleaning shop items: {e}")

    async def reset_shop_items(self):
        """Setzt die Shop-Items komplett zurück"""
        try:
            with closing(self.get_db_connection()) as conn:
                conn.execute("DELETE FROM shop_items")
                conn.commit()
                self.ensure_default_shop_items()
        except Exception as e:
            print(f"Error resetting shop items: {e}")

    def get_user_inventory(self, user_id):
        with closing(self.get_db_connection()) as conn:
            current_time = int(time.time())
            items = conn.execute("""
                SELECT
                    ui.item_id,
                    si.item_name,
                    si.description,
                    si.emoji,
                    ui.quantity,
                    ui.expires_at,
                    ui.used_at,
                    CASE
                        WHEN LOWER(si.item_name) = 'bankrob' AND ui.used_at IS NULL THEN 1
                        WHEN ui.expires_at > ? THEN 1
                        ELSE 0
                    END as is_active
                FROM user_inventory ui
                JOIN shop_items si ON ui.item_id = si.item_id
                WHERE ui.user_id = ? AND ui.quantity > 0
                ORDER BY
                    CASE
                        WHEN LOWER(si.item_name) = 'bankrob' AND ui.used_at IS NULL THEN 0
                        WHEN is_active = 1 THEN 1
                        ELSE 2
                    END,
                    si.item_name
            """, (current_time, user_id)).fetchall()
            return items

    @commands.Cog.listener()
    async def on_ready(self):
        """Ensure default items are added when bot starts"""
        self.ensure_default_shop_items()

    def get_user(self, user_id, username):
        """Get or create user datenbanken with proper default values"""
        with closing(self.get_db_connection()) as conn:
            user = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not user:
                conn.execute("""
                    INSERT INTO users (user_id, username, cash, bank, xp, level, 
                                    total_earned, total_spent, games_played, games_won)
                    VALUES (?, ?, 100, 0, 0, 1, 0, 0, 0, 0)
                """, (user_id, username))
                conn.commit()
                user = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

            # Convert to dict and ensure no None values for numeric fields
            user_dict = dict(user)
            numeric_fields = ['cash', 'bank', 'xp', 'level', 'total_earned',
                              'total_spent', 'games_played', 'games_won']
            for field in numeric_fields:
                if user_dict.get(field) is None:
                    user_dict[field] = 0

            return user_dict

    def format_cooldown(self, timestamp):
        """Format Unix timestamp for Discord"""
        return f"<t:{timestamp}:R>" if timestamp else "now"

    # --- Economy Commands ---
    # --- Economy Commands with Item Effects ---
    @app_commands.command(name="work", description="Work to earn money (1h cooldown)")
    @app_commands.guild_only()
    async def work(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)
            current_time = int(time.time())

            if user['work_cooldown'] and current_time < user['work_cooldown']:
                await interaction.followup.send(
                    f"⏳ Next work available {self.format_cooldown(user['work_cooldown'])}",
                    ephemeral=True
                )
                return

            # Base amount with laptop boost
            base_amount = random.randint(50, 200)
            amount = base_amount * 2 if self.has_active_item(interaction.user.id, "laptop") else base_amount

            # XP calculation with coffee boost
            xp_gain = 10
            if self.has_active_item(interaction.user.id, "coffee"):
                xp_gain = int(xp_gain * 1.5)  # 50% more XP

            cooldown = current_time + 3600  # 1 hour

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash + ?,
                    work_cooldown = ?,
                    total_earned = total_earned + ?,
                    xp = xp + ?
                    WHERE user_id = ?
                """, (amount, cooldown, amount, xp_gain, interaction.user.id))
                conn.commit()

            embed = discord.Embed(
                title="💼 Work Complete",
                description=f"Earned **{amount} coins**!\nNext work {self.format_cooldown(cooldown)}",
                color=discord.Color.green()
            )

            if self.has_active_item(interaction.user.id, "laptop"):
                embed.set_footer(text="💻 Laptop boost active (double earnings)")
            elif self.has_active_item(interaction.user.id, "coffee"):
                embed.set_footer(text="☕ Coffee boost active (50% more XP)")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="daily", description="Claim daily reward (24h cooldown)")
    @app_commands.guild_only()
    async def daily(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)
            current_time = int(time.time())

            if user['daily_cooldown'] and current_time < user['daily_cooldown']:
                await interaction.followup.send(
                    f"⏳ Next daily available {self.format_cooldown(user['daily_cooldown'])}",
                    ephemeral=True
                )
                return

            # XP calculation with coffee boost
            xp_gain = 15
            if self.has_active_item(interaction.user.id, "coffee"):
                xp_gain = int(xp_gain * 1.5)  # 50% more XP

            amount = random.randint(100, 500)
            cooldown = current_time + 86400  # 24 hours

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash + ?,
                    daily_cooldown = ?,
                    total_earned = total_earned + ?,
                    xp = xp + ?
                    WHERE user_id = ?
                """, (amount, cooldown, amount, xp_gain, interaction.user.id))
                conn.commit()

            embed = discord.Embed(
                title="🎁 Daily Reward",
                description=f"Claimed **{amount} coins**!\nNext daily {self.format_cooldown(cooldown)}",
                color=discord.Color.gold()
            )

            if self.has_active_item(interaction.user.id, "coffee"):
                embed.set_footer(text="☕ Coffee boost active (50% more XP)")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="stream", description="Stream to earn money (2h cooldown)")
    @app_commands.guild_only()
    async def stream(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)
            current_time = int(time.time())

            if user['stream_cooldown'] and current_time < user['stream_cooldown']:
                await interaction.followup.send(
                    f"⏳ Next stream available {self.format_cooldown(user['stream_cooldown'])}",
                    ephemeral=True
                )
                return

            amount = random.randint(200, 500)
            cooldown = current_time + 7200  # 2 hours

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash + ?,
                    stream_cooldown = ?,
                    total_earned = total_earned + ?,
                    xp = xp + 15
                    WHERE user_id = ?
                """, (amount, cooldown, amount, interaction.user.id))
                conn.commit()

            embed = discord.Embed(
                title="🎥 Stream Complete",
                description=f"Earned **{amount} coins** from streaming!\nNext stream {self.format_cooldown(cooldown)}",
                color=discord.Color.purple()
            )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    # --- Bank Commands ---
    @app_commands.command(name="deposit", description="Deposit money to bank (use 'all' for all-in)")
    @app_commands.guild_only()
    @app_commands.describe(amount="Amount to deposit (or 'all' for all cash)")
    async def deposit(self, interaction: discord.Interaction, amount: str):
        try:
            await interaction.response.defer()

            # Get user datenbanken
            user = self.get_user(interaction.user.id, interaction.user.name)

            # Handle all-in
            if amount.lower() == 'all':
                deposit_amount = user['cash']
                if deposit_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to deposit!", ephemeral=True)
                    return
            else:
                try:
                    deposit_amount = int(amount)
                    if deposit_amount <= 0:
                        await interaction.followup.send("❌ Amount must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            # Check if user has enough cash
            if user['cash'] < deposit_amount:
                await interaction.followup.send("❌ You don't have enough cash to deposit that amount!", ephemeral=True)
                return

            # Process deposit
            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                        cash = cash - ?,
                        bank = bank + ?
                    WHERE user_id = ?
                """, (deposit_amount, deposit_amount, interaction.user.id))
                conn.commit()

            # Create response embed
            embed = discord.Embed(
                title="🏦 Deposit Successful",
                description=f"Deposited **{deposit_amount} coins** to your bank!",
                color=discord.Color.green()
            )
            embed.add_field(name="New Balance",
                            value=f"💵 Cash: {user['cash'] - deposit_amount}\n🏦 Bank: {user['bank'] + deposit_amount}",
                            inline=False)

            if amount.lower() == 'all':
                embed.set_footer(text="💰 Deposited all your cash!")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    @app_commands.command(name="withdraw", description="Withdraw money from bank (use 'all' for all-in)")
    @app_commands.guild_only()
    @app_commands.describe(amount="Amount to withdraw (or 'all' for all bank)")
    async def withdraw(self, interaction: discord.Interaction, amount: str):
        try:
            await interaction.response.defer()

            # Get user datenbanken
            user = self.get_user(interaction.user.id, interaction.user.name)

            # Handle all-in
            if amount.lower() == 'all':
                withdraw_amount = user['bank']
                if withdraw_amount <= 0:
                    await interaction.followup.send("❌ You don't have any money in the bank!", ephemeral=True)
                    return
            else:
                try:
                    withdraw_amount = int(amount)
                    if withdraw_amount <= 0:
                        await interaction.followup.send("❌ Amount must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            # Check if user has enough in bank
            if user['bank'] < withdraw_amount:
                await interaction.followup.send("❌ You don't have enough money in the bank!", ephemeral=True)
                return

            # Process withdrawal
            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                        bank = bank - ?,
                        cash = cash + ?
                    WHERE user_id = ?
                """, (withdraw_amount, withdraw_amount, interaction.user.id))
                conn.commit()

            # Create response embed
            embed = discord.Embed(
                title="🏦 Withdrawal Successful",
                description=f"Withdrew **{withdraw_amount} coins** from your bank!",
                color=discord.Color.green()
            )
            embed.add_field(name="New Balance",
                            value=f"💵 Cash: {user['cash'] + withdraw_amount}\n🏦 Bank: {user['bank'] - withdraw_amount}",
                            inline=False)

            if amount.lower() == 'all':
                embed.set_footer(text="💰 Withdrew all your bank balance!")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
    # --- Gambling Commands ---
    # --- Gambling Commands with Dice Boost ---
    @app_commands.command(name="dice", description="Play dice against the bot")
    @app_commands.guild_only()
    @app_commands.describe(amount="Bet amount (or 'all' for all-in)")
    async def dice(self, interaction: discord.Interaction, amount: str):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)

            # All-in handling
            if amount.lower() == 'all':
                bet_amount = user['cash']
                if bet_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to go all-in!", ephemeral=True)
                    return
            else:
                try:
                    bet_amount = int(amount)
                    if bet_amount <= 0:
                        await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            if user['cash'] < bet_amount:
                await interaction.followup.send("❌ You don't have enough cash!", ephemeral=True)
                return

            # Check for lucky dice boost
            dice_boost = self.has_active_item(interaction.user.id, "dice")

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                        cash = cash - ?,
                        total_spent = total_spent + ?,
                        games_played = games_played + 1
                    WHERE user_id = ?
                """, (bet_amount, bet_amount, interaction.user.id))

                user_roll = random.randint(1, 6)
                bot_roll = random.randint(1, 6)

                # Apply dice boost if active
                if dice_boost:
                    user_roll = min(6, user_roll + 1)  # +1 to user's roll

                if user_roll > bot_roll:
                    win_amount = bet_amount * 2
                    conn.execute("""
                        UPDATE users SET 
                            cash = cash + ?,
                            total_earned = total_earned + ?,
                            games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))
                    result_text = f"🎲 **{user_roll} vs {bot_roll}** - Won! +{win_amount} coins"
                    color = discord.Color.green()
                elif user_roll == bot_roll:
                    conn.execute("UPDATE users SET cash = cash + ? WHERE user_id = ?",
                                 (bet_amount, interaction.user.id))
                    result_text = f"🎲 **{user_roll} vs {bot_roll}** - Draw (Bet returned)"
                    color = discord.Color.orange()
                else:
                    result_text = f"🎲 **{user_roll} vs {bot_roll}** - Lost (-{bet_amount} coins)"
                    color = discord.Color.red()

                conn.commit()

            embed = discord.Embed(description=result_text, color=color)
            if amount.lower() == 'all':
                embed.set_footer(text="🎲 All-in bet!")
            if dice_boost:
                embed.set_footer(
                    text=(embed.footer.text + " 🎲 Lucky dice active!") if embed.footer.text else "🎲 Lucky dice active!")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    @app_commands.command(name="roulette", description="Bet on red/black")
    @app_commands.guild_only()
    @app_commands.describe(
        color="Color to bet on",
        amount="Bet amount (or 'all' for all-in)"
    )
    @app_commands.choices(color=[
        app_commands.Choice(name="Red", value="red"),
        app_commands.Choice(name="Black", value="black")
    ])
    async def roulette(self, interaction: discord.Interaction, color: app_commands.Choice[str], amount: str):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)

            # All-in handling
            if amount.lower() == 'all':
                bet_amount = user['cash']
                if bet_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to go all-in!", ephemeral=True)
                    return
            else:
                try:
                    bet_amount = int(amount)
                    if bet_amount <= 0:
                        await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            if user['cash'] < bet_amount:
                await interaction.followup.send("❌ You don't have enough cash!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    games_played = games_played + 1
                    WHERE user_id = ?
                """, (bet_amount, bet_amount, interaction.user.id))

                # Simulate roulette spin (37 numbers, 0-36)
                winning_number = random.randint(0, 36)

                # Determine winning color (0 is green, odd=red, even=black)
                if winning_number == 0:
                    winning_color = "green"
                elif winning_number % 2 == 1:
                    winning_color = "red"
                else:
                    winning_color = "black"

                if color.value == winning_color:
                    win_amount = bet_amount * 2
                    conn.execute("""
                        UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?,
                        games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))
                    result_text = f"🎰 **{winning_color.capitalize()} {winning_number}** - Won! +{win_amount} coins"
                    color_embed = discord.Color.green()
                else:
                    result_text = f"🎰 **{winning_color.capitalize()} {winning_number}** - Lost (-{bet_amount} coins)"
                    color_embed = discord.Color.red()

                conn.commit()

            embed = discord.Embed(description=result_text, color=color_embed)
            if amount.lower() == 'all':
                embed.set_footer(text="🎰 All-in bet!")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    @app_commands.command(name="slots", description="Play slot machine")
    @app_commands.guild_only()
    @app_commands.describe(amount="Bet amount (or 'all' for all-in)")
    async def slots(self, interaction: discord.Interaction, amount: str):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)

            # All-in handling
            if amount.lower() == 'all':
                bet_amount = user['cash']
                if bet_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to go all-in!", ephemeral=True)
                    return
            else:
                try:
                    bet_amount = int(amount)
                    if bet_amount <= 0:
                        await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            if user['cash'] < bet_amount:
                await interaction.followup.send("❌ You don't have enough cash!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    games_played = games_played + 1
                    WHERE user_id = ?
                """, (bet_amount, bet_amount, interaction.user.id))

                symbols = ["🍒", "🍋", "🍊", "🍇", "🍉", "7️⃣"]
                reels = [random.choice(symbols) for _ in range(3)]
                result_text = " | ".join(reels)

                if reels[0] == reels[1] == reels[2]:
                    win_multiplier = 10 if reels[0] == "7️⃣" else 5
                    win_amount = bet_amount * win_multiplier
                    conn.execute("""
                        UPDATE users SET 
                            cash = cash + ?,
                            total_earned = total_earned + ?,
                            games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))
                    outcome = f"🎰 {result_text} - JACKPOT! Won {win_amount} coins!" if reels[
                                                                                           0] == "7️⃣" else f"🎰 {result_text} - Triple! Won {win_amount} coins!"
                    color = discord.Color.gold() if reels[0] == "7️⃣" else discord.Color.green()
                elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
                    win_amount = bet_amount * 2
                    conn.execute("""
                        UPDATE users SET 
                            cash = cash + ?,
                            total_earned = total_earned + ?,
                            games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))
                    outcome = f"🎰 {result_text} - Double! Won {win_amount} coins!"
                    color = discord.Color.green()
                else:
                    outcome = f"🎰 {result_text} - Lost {bet_amount} coins"
                    color = discord.Color.red()

                conn.commit()

            embed = discord.Embed(description=outcome, color=color)
            if amount.lower() == 'all':
                embed.set_footer(text="🎰 All-in bet!")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    @app_commands.command(name="coinflip2", description="Flip a coin for double or nothing")
    @app_commands.guild_only()
    @app_commands.describe(
        choice="Heads or tails",
        amount="Bet amount"
    )
    @app_commands.choices(choice=[
        app_commands.Choice(name="Heads", value="heads"),
        app_commands.Choice(name="Tails", value="tails")
    ])
    async def coinflip2(self, interaction: discord.Interaction, choice: app_commands.Choice[str], amount: int):
        try:
            await interaction.response.defer()
            if amount <= 0:
                await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                current_cash = conn.execute("SELECT cash FROM users WHERE user_id = ?",
                                            (interaction.user.id,)).fetchone()
                if not current_cash or current_cash[0] < amount:
                    await interaction.followup.send("❌ You don't have enough coins to place that bet!", ephemeral=True)
                    return

                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    games_played = games_played + 1
                    WHERE user_id = ?
                """, (amount, amount, interaction.user.id))

                result_str = random.choice(["heads", "tails"])
                if choice.value == result_str:
                    win = amount * 2
                    conn.execute("""
                        UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?,
                        games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win, win, interaction.user.id))
                    outcome = f"🪙 **{result_str.capitalize()}** - You won {win} coins!"
                    color = discord.Color.green()
                else:
                    outcome = f"🪙 **{result_str.capitalize()}** - You lost {amount} coins"
                    color = discord.Color.red()

                conn.commit()

            await interaction.followup.send(embed=discord.Embed(description=outcome, color=color))

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="blackjack", description="Play blackjack against the bot")
    @app_commands.guild_only()
    @app_commands.describe(amount="Bet amount (or 'all' for all-in)")
    async def blackjack(self, interaction: discord.Interaction, amount: str):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)

            # All-in handling
            if amount.lower() == 'all':
                bet_amount = user['cash']
                if bet_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to go all-in!", ephemeral=True)
                    return
            else:
                try:
                    bet_amount = int(amount)
                    if bet_amount <= 0:
                        await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            if user['cash'] < bet_amount:
                await interaction.followup.send("❌ You don't have enough cash!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    games_played = games_played + 1
                    WHERE user_id = ?
                """, (bet_amount, bet_amount, interaction.user.id))
                conn.commit()

            # Initialize game
            deck = [2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11] * 4
            random.shuffle(deck)

            player_hand = [deck.pop(), deck.pop()]
            dealer_hand = [deck.pop(), deck.pop()]

            def hand_value(hand):
                value = sum(hand)
                aces = hand.count(11)
                while value > 21 and aces:
                    value -= 10
                    aces -= 1
                return value

            player_value = hand_value(player_hand)

            # Create initial embed
            embed = discord.Embed(
                title=f"🃏 Blackjack (Bet: {bet_amount} coins)",
                color=discord.Color.dark_green()
            )
            embed.add_field(
                name="Your Hand",
                value=f"{' '.join(str(c) for c in player_hand)} (Total: {player_value})",
                inline=False
            )
            embed.add_field(
                name="Dealer's Hand",
                value=f"{dealer_hand[0]} ?",
                inline=False
            )
            if amount.lower() == 'all':
                embed.set_footer(text="🃏 All-in bet!")

            # Check for immediate blackjack
            if player_value == 21:
                win_amount = int(bet_amount * 2.5)
                with closing(self.get_db_connection()) as conn:
                    conn.execute("""
                        UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?,
                        games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))
                    conn.commit()

                embed.add_field(
                    name="Result",
                    value=f"Blackjack! You won {win_amount} coins!",
                    inline=False
                )
                embed.color = discord.Color.gold()
                return await interaction.followup.send(embed=embed)

            class BlackjackView(discord.ui.View):
                def __init__(self, game):
                    super().__init__(timeout=30)
                    self.game = game

                async def interaction_check(self, interaction: discord.Interaction) -> bool:
                    return interaction.user == self.game.interaction.user

                async def on_timeout(self):
                    for child in self.children:
                        child.disabled = True
                    await self.game.message.edit(view=self)

                @discord.ui.button(label="Hit", style=discord.ButtonStyle.green)
                async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
                    await self.game.player_hit(interaction)

                @discord.ui.button(label="Stand", style=discord.ButtonStyle.red)
                async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
                    await self.game.player_stand(interaction)

            class BlackjackGame:
                def __init__(self, cog, interaction, player_hand, dealer_hand, deck, amount):
                    self.cog = cog
                    self.interaction = interaction
                    self.player_hand = player_hand
                    self.dealer_hand = dealer_hand
                    self.deck = deck
                    self.amount = amount
                    self.message = None
                    self.view = None

                async def player_hit(self, interaction):
                    self.player_hand.append(self.deck.pop())
                    player_value = hand_value(self.player_hand)

                    embed = interaction.message.embeds[0]
                    embed.set_field_at(
                        0,
                        name="Your Hand",
                        value=f"{' '.join(str(c) for c in self.player_hand)} (Total: {player_value})",
                        inline=False
                    )

                    if player_value > 21:
                        embed.add_field(
                            name="Result",
                            value=f"Bust! You lost {self.amount} coins",
                            inline=False
                        )
                        embed.color = discord.Color.red()
                        for child in self.view.children:
                            child.disabled = True
                        await interaction.response.edit_message(embed=embed, view=self.view)
                        return

                    await interaction.response.edit_message(embed=embed)

                async def player_stand(self, interaction):
                    dealer_value = hand_value(self.dealer_hand)
                    while dealer_value < 17:
                        self.dealer_hand.append(self.deck.pop())
                        dealer_value = hand_value(self.dealer_hand)

                    player_value = hand_value(self.player_hand)

                    embed = interaction.message.embeds[0]
                    embed.set_field_at(
                        0,
                        name="Your Hand",
                        value=f"{' '.join(str(c) for c in self.player_hand)} (Total: {player_value})",
                        inline=False
                    )
                    embed.set_field_at(
                        1,
                        name="Dealer's Hand",
                        value=f"{' '.join(str(c) for c in self.dealer_hand)} (Total: {dealer_value})",
                        inline=False
                    )

                    if dealer_value > 21 or player_value > dealer_value:
                        win_amount = self.amount * 2
                        with closing(self.cog.get_db_connection()) as conn:
                            conn.execute("""
                                UPDATE users SET 
                                cash = cash + ?,
                                total_earned = total_earned + ?,
                                games_won = games_won + 1
                                WHERE user_id = ?
                            """, (win_amount, win_amount, interaction.user.id))
                            conn.commit()
                        embed.add_field(
                            name="Result",
                            value=f"You won {win_amount} coins!",
                            inline=False
                        )
                        embed.color = discord.Color.green()
                    elif player_value == dealer_value:
                        with closing(self.cog.get_db_connection()) as conn:
                            conn.execute("""
                                UPDATE users SET 
                                cash = cash + ?
                                WHERE user_id = ?
                            """, (self.amount, interaction.user.id))
                            conn.commit()
                        embed.add_field(
                            name="Result",
                            value="Push! Your bet was returned",
                            inline=False
                        )
                        embed.color = discord.Color.orange()
                    else:
                        embed.add_field(
                            name="Result",
                            value=f"You lost {self.amount} coins",
                            inline=False
                        )
                        embed.color = discord.Color.red()

                    for child in self.view.children:
                        child.disabled = True

                    await interaction.response.edit_message(embed=embed, view=self.view)

            game = BlackjackGame(self, interaction, player_hand, dealer_hand, deck, bet_amount)
            view = BlackjackView(game)
            game.view = view
            game.message = await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    @app_commands.command(name="highlow", description="Guess if the next card will be higher or lower")
    @app_commands.guild_only()
    @app_commands.describe(
        guess="Higher or lower",
        amount="Bet amount (or 'all' for all-in)"
    )
    @app_commands.choices(guess=[
        app_commands.Choice(name="Higher", value="higher"),
        app_commands.Choice(name="Lower", value="lower"),
        app_commands.Choice(name="Same", value="same")
    ])
    async def highlow(self, interaction: discord.Interaction, guess: app_commands.Choice[str], amount: str):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)

            # All-in handling
            if amount.lower() == 'all':
                bet_amount = user['cash']
                if bet_amount <= 0:
                    await interaction.followup.send("❌ You don't have any cash to go all-in!", ephemeral=True)
                    return
            else:
                try:
                    bet_amount = int(amount)
                    if bet_amount <= 0:
                        await interaction.followup.send("❌ Bet must be positive!", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("❌ Amount must be a number or 'all'!", ephemeral=True)
                    return

            if user['cash'] < bet_amount:
                await interaction.followup.send("❌ You don't have enough cash!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    games_played = games_played + 1
                    WHERE user_id = ?
                """, (bet_amount, bet_amount, interaction.user.id))

                # Create deck and draw cards
                deck = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14] * 4  # 11=J, 12=Q, 13=K, 14=A
                random.shuffle(deck)

                first_card = deck.pop()
                second_card = deck.pop()

                # Determine outcome
                if first_card == second_card:
                    result = "same"
                elif second_card > first_card:
                    result = "higher"
                else:
                    result = "lower"

                # Card display names
                card_names = {
                    2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7", 8: "8",
                    9: "9", 10: "10", 11: "J", 12: "Q", 13: "K", 14: "A"
                }

                if guess.value == result:
                    if result == "same":
                        win_amount = bet_amount * 5  # Higher payout for guessing "same"
                    else:
                        win_amount = bet_amount * 2

                    conn.execute("""
                        UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?,
                        games_won = games_won + 1
                        WHERE user_id = ?
                    """, (win_amount, win_amount, interaction.user.id))

                    outcome = f"🎴 {card_names[first_card]} → {card_names[second_card]} - Correct! Won {win_amount} coins!"
                    color = discord.Color.green()
                else:
                    outcome = f"🎴 {card_names[first_card]} → {card_names[second_card]} - Wrong guess. Lost {bet_amount} coins"
                    color = discord.Color.red()

                conn.commit()

            embed = discord.Embed(description=outcome, color=color)
            if amount.lower() == 'all':
                embed.set_footer(text="🎴 All-in bet!")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)
    # --- Social Commands ---
    @app_commands.command(name="give", description="Give coins to another user")
    @app_commands.guild_only()
    @app_commands.describe(
        user="Recipient",
        amount="Amount to give"
    )
    async def give(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        try:
            await interaction.response.defer()
            if amount <= 0:
                await interaction.followup.send("❌ Amount must be positive!", ephemeral=True)
                return
            if user.bot:
                await interaction.followup.send("❌ Bots can't receive money!", ephemeral=True)
                return
            if user.id == interaction.user.id:
                await interaction.followup.send("❌ You can't give to yourself!", ephemeral=True)
                return

            with closing(self.get_db_connection()) as conn:
                cursor = conn.execute("SELECT cash FROM users WHERE user_id = ?", (interaction.user.id,))
                result = cursor.fetchone()
                if not result:
                    await interaction.followup.send("❌ You don't have an account!", ephemeral=True)
                    return

                user_cash = result[0]
                if user_cash < amount:
                    await interaction.followup.send("❌ You don't have enough coins!", ephemeral=True)
                    return

                # Begin transaction
                conn.execute("""
                    UPDATE users SET 
                        cash = cash - ?,
                        total_spent = total_spent + ?
                    WHERE user_id = ?
                """, (amount, amount, interaction.user.id))

                conn.execute("""
                    UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?
                    WHERE user_id = ?
                """, (amount, amount, user.id))

                conn.commit()

            await interaction.followup.send(f"🎁 You gave {user.mention} **{amount} coins**!")

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="steal", description="Attempt to steal coins (4h cooldown)")
    @app_commands.guild_only()
    async def steal(self, interaction: discord.Interaction, user: discord.Member):
        try:
            await interaction.response.defer()

            # Basic checks
            if user.bot:
                return await interaction.followup.send("❌ Bots have no money!", ephemeral=True)
            if user.id == interaction.user.id:
                return await interaction.followup.send("❌ You can't steal from yourself!", ephemeral=True)

            current_time = int(time.time())
            thief = self.get_user(interaction.user.id, interaction.user.name)
            victim = self.get_user(user.id, user.name)

            # Cooldown check
            if thief['steal_cooldown'] and current_time < thief['steal_cooldown']:
                return await interaction.followup.send(
                    f"⏳ Next steal attempt {self.format_cooldown(thief['steal_cooldown'])}",
                    ephemeral=True
                )

            # Shield protection
            if self.has_active_item(user.id, "shield"):
                shield_item = self.get_active_item(user.id, "shield")
                remaining = shield_item['expires_at'] - current_time
                return await interaction.followup.send(
                    f"🛡️ {user.display_name} is protected by a shield! (Expires in {self.format_time_remaining(remaining)})",
                    ephemeral=True
                )

            # Bankrob check – Bankrob is active if present and not yet used.
            bankrob_active = False
            bankrob_item = self.get_active_item(interaction.user.id, "bankrob")
            if bankrob_item and bankrob_item.get('used_at') is None:
                bankrob_active = True

            with closing(self.get_db_connection()) as conn:
                try:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    if bankrob_active:
                        # Bank robbery logic
                        if victim['bank'] < 10:
                            conn.rollback()
                            return await interaction.followup.send("❌ Target has no money in the bank!", ephemeral=True)
                        max_amount = min(int(victim['bank'] * 0.3), 10000)
                        stolen_amount = random.randint(100, max_amount)
                        source = "bank"
                    else:
                        # Normal steal logic from cash
                        if victim['cash'] < 10:
                            conn.rollback()
                            return await interaction.followup.send("❌ Target has no cash!", ephemeral=True)
                        stolen_amount = min(random.randint(10, 1000), victim['cash'])
                        source = "cash"

                    if stolen_amount <= 0:
                        conn.rollback()
                        return await interaction.followup.send("❌ Invalid steal amount!", ephemeral=True)

                    # Simulate success with 40% chance
                    success = random.random() > 0.6

                    if success:
                        # Deduct money from victim
                        if bankrob_active:
                            conn.execute("""
                                UPDATE users SET 
                                    bank = bank - ?,
                                    total_spent = total_spent + ?
                                WHERE user_id = ?
                            """, (stolen_amount, stolen_amount, user.id))
                        else:
                            conn.execute("""
                                UPDATE users SET 
                                    cash = cash - ?,
                                    total_spent = total_spent + ?
                                WHERE user_id = ?
                            """, (stolen_amount, stolen_amount, user.id))

                        # Credit the thief
                        conn.execute("""
                            UPDATE users SET 
                                cash = cash + ?,
                                total_earned = total_earned + ?,
                                steal_cooldown = ?,
                                xp = xp + 10
                            WHERE user_id = ?
                        """, (stolen_amount, stolen_amount, current_time + 14400, interaction.user.id))

                        # Bei Bankrob: Nutze das Item, das erst jetzt verbraucht wird
                        if bankrob_active:
                            # Hier übergeben wir den String "bankrob" als Identifier an use_item_by_id
                            if not await self.use_item_by_id(interaction.user.id, "bankrob"):
                                conn.rollback()
                                return await interaction.followup.send("❌ Failed to use bankrob item!", ephemeral=True)

                        result = f"🦹 Steal successful! You stole **{stolen_amount} coins** from {user.mention}'s {source}!"
                        color = discord.Color.green()
                    else:
                        # On failure, set cooldown and award minimal XP
                        conn.execute("""
                            UPDATE users SET 
                                steal_cooldown = ?,
                                xp = xp + 3
                            WHERE user_id = ?
                        """, (current_time + 14400, interaction.user.id))
                        result = f"🦹 Steal failed! {user.mention} caught you!"
                        color = discord.Color.red()

                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e

            embed = discord.Embed(description=result, color=color)
            if bankrob_active:
                embed.set_footer(text="💰 Bank robbery kit was used (one-time use)")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

            # --- Stats Commands ---
    @app_commands.command(name="balance", description="Show your or another user's balance")
    @app_commands.guild_only()
    @app_commands.describe(user="Optional: User to check balance of")
    async def balance(self, interaction: discord.Interaction, user: discord.Member = None):
        try:
            await interaction.response.defer()
            target_user = user if user else interaction.user
            user_data = self.get_user(target_user.id, target_user.name)

            embed = discord.Embed(
                title=f"💰 {target_user.display_name}'s Balance",
                description=f"💵 **Cash:** {user_data['cash']} coins\n🏦 **Bank:** {user_data['bank']} coins\n💳 **Total:** {user_data['cash'] + user_data['bank']} coins",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=target_user.display_avatar.url)
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="top", description="Show the top 10 richest users on the server")
    @app_commands.guild_only()
    async def top(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()

            with closing(self.get_db_connection()) as conn:
                # Get top 10 users by total wealth (cash + bank), grouped by user_id
                top_users = conn.execute("""
                    SELECT user_id, MAX(username) as username, SUM(cash) + SUM(bank) as total
                    FROM users
                    GROUP BY user_id
                    ORDER BY total DESC
                    LIMIT 10
                """).fetchall()

            if not top_users:
                await interaction.followup.send("❌ No user data available yet!", ephemeral=True)
                return

            # Create leaderboard
            leaderboard = []
            members_cache = {}

            # First try to fetch all members at once for efficiency
            try:
                members = await interaction.guild.fetch_members(limit=None).flatten()
                members_cache = {member.id: member for member in members}
            except:
                pass

            for idx, user in enumerate(top_users, 1):
                user_id = user['user_id']

                # Try to get member from cache first
                member = members_cache.get(user_id)

                if member:
                    name = member.display_name
                else:
                    # Fallback to direct fetch if not in cache
                    try:
                        member = await interaction.guild.fetch_member(user_id)
                        name = member.display_name
                    except:
                        name = user['username']

                leaderboard.append(f"`{idx}.` {name} - **{user['total']} coins**")

            embed = discord.Embed(
                title="🏆 Top 10 Richest Users",
                description="\n".join(leaderboard),
                color=discord.Color.gold()
            )
            embed.set_footer(text="Total wealth = Cash + Bank")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="level", description="Show your level")
    @app_commands.guild_only()
    async def level(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            user = self.get_user(interaction.user.id, interaction.user.name)
            current_level = user.get('level', 1)
            next_level_xp = (current_level + 1) ** 2 * 100

            embed = discord.Embed(
                title=f"⭐ Level {current_level}",
                description=f"**XP:** {user['xp']}/{next_level_xp}\n**Progress:** {round((user['xp'] / next_level_xp) * 100, 1)}%",
                color=discord.Color.gold()
            )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send("❌ Error: " + str(e), ephemeral=True)

    @app_commands.command(name="stats", description="Show your or another user's statistics")
    @app_commands.guild_only()
    @app_commands.describe(user="Optional: User to check stats of")
    async def stats(self, interaction: discord.Interaction, user: discord.Member = None):
        try:
            await interaction.response.defer()
            target_user = user if user else interaction.user
            user_data = self.get_user(target_user.id, target_user.name)
            current_level = user_data.get('level', 1)
            next_level_xp = (current_level + 1) ** 2 * 100

            games_won = user_data.get('games_won', 0)
            games_played = max(user_data.get('games_played', 1), 1)  # Avoid division by zero
            win_rate = (games_won / games_played * 100)

            embed = discord.Embed(
                title=f"📊 {target_user.display_name}'s Stats",
                color=discord.Color.blurple()
            )
            embed.set_thumbnail(url=target_user.display_avatar.url)

            embed.add_field(name="💰 Finances",
                            value=f"💵 Cash: {user_data['cash']}\n🏦 Bank: {user_data['bank']}\n💸 Earned: {user_data['total_earned']}\n💳 Spent: {user_data['total_spent']}",
                            inline=False)
            embed.add_field(name="🎮 Games",
                            value=f"🎲 Played: {games_played}\n🏆 Won: {games_won}\n📊 Win rate: {round(win_rate, 1)}%",
                            inline=False)
            embed.add_field(name="⭐ Level",
                            value=f"Level: {current_level}\nXP: {user_data['xp']}/{next_level_xp}\nProgress: {round((user_data['xp'] / next_level_xp) * 100, 1)}%",
                            inline=False)

            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    # --- New Shop System ---
    @app_commands.command(name="shop", description="View and buy items from the shop")
    @app_commands.guild_only()
    async def shop(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()

            with closing(self.get_db_connection()) as conn:
                # Get user balance
                user = conn.execute("""
                    SELECT cash, bank FROM users WHERE user_id = ?
                """, (interaction.user.id,)).fetchone()

                if not user:
                    await interaction.followup.send("❌ You don't have an account!", ephemeral=True)
                    return

                shop_items = conn.execute("""
                    SELECT item_id, item_name, price, description, emoji 
                    FROM shop_items 
                    WHERE guild_id = ?
                    ORDER BY price
                """, (interaction.guild.id,)).fetchall()

            if not shop_items:
                await interaction.followup.send(
                    "❌ The shop is empty! Admins can add items with `/configureshop`",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="🛒 Shop",
                description=f"💵 Cash: {user['cash']} coins\n🏦 Bank: {user['bank']} coins\n\nSelect an item to purchase:",
                color=discord.Color.gold()
            )

            for item in shop_items:
                embed.add_field(
                    name=f"{item['emoji']} {item['item_name']} - {item['price']} coins",
                    value=item['description'],
                    inline=False
                )

            view = self.ShopView(shop_items, self, user['cash'], user['bank'])
            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    class ShopItemDropdown(discord.ui.Select):
        def __init__(self, shop_items, cog, user_cash, user_bank):
            options = []
            self.cog = cog
            self.user_cash = user_cash
            self.user_bank = user_bank

            for item in shop_items:
                options.append(discord.SelectOption(
                    label=f"{item['emoji']} {item['item_name']} - {item['price']} coins",
                    description=item['description'],
                    value=str(item['item_id']),
                    emoji=item['emoji']
                ))

            super().__init__(
                placeholder="Select an item to purchase...",
                min_values=1,
                max_values=1,
                options=options
            )

        async def callback(self, interaction: discord.Interaction):
            try:
                await interaction.response.defer(ephemeral=True, thinking=True)
                item_id = int(self.values[0])
                cog = self.cog

                # Get item details first
                with closing(cog.get_db_connection()) as conn:
                    item = conn.execute("""
                        SELECT * FROM shop_items WHERE item_id = ?
                    """, (item_id,)).fetchone()

                    if not item:
                        await interaction.followup.send("❌ That item doesn't exist!", ephemeral=True)
                        return

                    # Check user balance
                    user = conn.execute("""
                        SELECT cash, bank FROM users WHERE user_id = ?
                    """, (interaction.user.id,)).fetchone()

                    if not user:
                        await interaction.followup.send("❌ You don't have an account!", ephemeral=True)
                        return

                    price = item['price']
                    cash = user['cash']
                    bank = user['bank']
                    total_funds = cash + bank

                    if total_funds < price:
                        await interaction.followup.send(
                            f"❌ You don't have enough coins! (Need: {price}, Have: {total_funds})",
                            ephemeral=True
                        )
                        return

                    # Process payment
                    if cash >= price:
                        new_cash = cash - price
                        new_bank = bank
                        payment_method = "cash"
                    else:
                        remaining = price - cash
                        new_cash = 0
                        new_bank = bank - remaining
                        payment_method = "cash and bank"

                    # Update user balance
                    conn.execute("""
                        UPDATE users SET 
                        cash = ?,
                        bank = ?,
                        total_spent = total_spent + ?
                        WHERE user_id = ?
                    """, (new_cash, new_bank, price, interaction.user.id))
                    conn.commit()

                # Add item to inventory (with retry logic)
                item_data = cog.DEFAULT_SHOP_ITEMS.get(item['item_name'].lower(), {})
                success = await cog.add_item_to_inventory(
                    interaction.user.id,
                    item['item_id'],
                    item_data.get('duration')
                )

                if not success:
                    # If adding to inventory failed, refund the money
                    with closing(cog.get_db_connection()) as conn:
                        conn.execute("""
                            UPDATE users SET 
                            cash = cash + ?,
                            bank = bank + ?,
                            total_spent = total_spent - ?
                            WHERE user_id = ?
                        """, (price - new_bank, new_bank, price, interaction.user.id))
                        conn.commit()

                    await interaction.followup.send("❌ Failed to add item to inventory! Your coins have been refunded.",
                                                    ephemeral=True)
                    return

                payment_msg = ""
                if payment_method == "cash and bank":
                    payment_msg = f" (Paid with 💵 {cash} cash + 🏦 {price - cash} bank)"
                elif payment_method == "cash":
                    payment_msg = " (Paid with 💵 cash)"

                embed = discord.Embed(
                    title="🛒 Purchase Successful",
                    description=f"You bought {item['emoji']} **{item['item_name']}** for {item['price']} coins{payment_msg}!\n{item['description']}",
                    color=discord.Color.green()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                await interaction.followup.send(f"❌ Error processing purchase: {str(e)}", ephemeral=True)

    class ShopView(discord.ui.View):
        def __init__(self, shop_items, cog, user_cash, user_bank):
            super().__init__()
            self.add_item(EconomyCog.ShopItemDropdown(shop_items, cog, user_cash, user_bank))
    # --- New Lottery System ---
    @app_commands.command(name="lottery", description="Buy a lottery ticket (1k coins)")
    @app_commands.guild_only()
    async def lottery(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            ticket_price = 1000
            user = self.get_user(interaction.user.id, interaction.user.name)

            if user['cash'] < ticket_price:
                await interaction.followup.send("❌ You need 1,000 coins for a ticket!", ephemeral=True)
                return

            # Simulate lottery draw
            win = random.choices(
                [0, 5000, 10000, 50000],
                weights=[0.7, 0.2, 0.08, 0.02],
                k=1
            )[0]

            with closing(self.get_db_connection()) as conn:
                if win > 0:
                    conn.execute("""
                        UPDATE users SET 
                        cash = cash - ? + ?,
                        total_spent = total_spent + ?,
                        total_earned = total_earned + ?,
                        games_played = games_played + 1,
                        games_won = games_won + 1
                        WHERE user_id = ?
                    """, (ticket_price, win, ticket_price, win, interaction.user.id))
                    result = f"🎫 You won **{win} coins**!"
                    color = discord.Color.gold()
                else:
                    conn.execute("""
                        UPDATE users SET 
                        cash = cash - ?,
                        total_spent = total_spent + ?,
                        games_played = games_played + 1
                        WHERE user_id = ?
                    """, (ticket_price, ticket_price, interaction.user.id))
                    result = "🎫 No win this time. Better luck next time!"
                    color = discord.Color.red()

                conn.commit()

            embed = discord.Embed(description=result, color=color)
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    # --- New Boost System ---
    @app_commands.command(name="boost", description="Spend coins to boost the server")
    @app_commands.guild_only()
    @app_commands.describe(amount="Amount to boost (min 1k)")
    async def boost(self, interaction: discord.Interaction, amount: int):
        try:
            await interaction.response.defer()
            if amount < 1000:
                await interaction.followup.send("❌ Minimum is 1,000 coins!", ephemeral=True)
                return

            user = self.get_user(interaction.user.id, interaction.user.name)
            if user['cash'] < amount:
                await interaction.followup.send("❌ You don't have enough coins!", ephemeral=True)
                return

            # 10% XP bonus for the booster
            xp_bonus = amount // 10

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    xp = xp + ?
                    WHERE user_id = ?
                """, (amount, amount, xp_bonus, interaction.user.id))
                conn.commit()

            embed = discord.Embed(
                title="🚀 Server Boost",
                description=f"You invested **{amount} coins** in the server!\n"
                            f"As a thank you, you received **{xp_bonus} XP**!",
                color=discord.Color.purple()
            )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    # --- New XP Purchase ---
    @app_commands.command(name="buyxp", description="Convert coins to XP")
    @app_commands.guild_only()
    @app_commands.describe(amount="Amount of coins to convert (100 coins = 1 XP)")
    async def buyxp(self, interaction: discord.Interaction, amount: int):
        try:
            await interaction.response.defer()
            if amount < 100:
                await interaction.followup.send("❌ Minimum is 100 coins (1 XP)!", ephemeral=True)
                return

            user = self.get_user(interaction.user.id, interaction.user.name)
            if user['cash'] < amount:
                await interaction.followup.send("❌ You don't have enough coins!", ephemeral=True)
                return

            xp_gain = amount // 100

            with closing(self.get_db_connection()) as conn:
                conn.execute("""
                    UPDATE users SET 
                    cash = cash - ?,
                    total_spent = total_spent + ?,
                    xp = xp + ?
                    WHERE user_id = ?
                """, (amount, amount, xp_gain, interaction.user.id))
                conn.commit()

            embed = discord.Embed(
                title="🧠 XP Purchase",
                description=f"You converted **{amount} coins** into **{xp_gain} XP**!",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    # --- Improved Admin Shop Configuration ---
    class ShopItemModal(discord.ui.Modal, title="Add Shop Item"):
        def __init__(self, cog, guild_id):
            super().__init__()
            self.cog = cog
            self.guild_id = guild_id

            self.item_name = discord.ui.TextInput(
                label="Item Name",
                placeholder="Enter the item name",
                required=True
            )

            self.price = discord.ui.TextInput(
                label="Price",
                placeholder="Enter the item price in coins",
                required=True
            )

            self.description = discord.ui.TextInput(
                label="Description",
                placeholder="Enter a description for the item",
                style=discord.TextStyle.long,
                required=True
            )

            self.emoji = discord.ui.TextInput(
                label="Emoji",
                placeholder="Enter an emoji to represent the item",
                required=True,
                max_length=5
            )

            self.add_item(self.item_name)
            self.add_item(self.price)
            self.add_item(self.description)
            self.add_item(self.emoji)

        async def on_submit(self, interaction: discord.Interaction):
            try:
                price = int(self.price.value)
                if price <= 0:
                    await interaction.response.send_message("❌ Price must be positive!", ephemeral=True)
                    return

                with closing(self.cog.get_db_connection()) as conn:
                    conn.execute("""
                        INSERT INTO shop_items 
                        (guild_id, item_name, price, description, emoji)
                        VALUES (?, ?, ?, ?, ?)
                    """, (self.guild_id, self.item_name.value, price,
                          self.description.value, self.emoji.value))
                    conn.commit()

                embed = discord.Embed(
                    title="🛒 Shop Item Added",
                    description=f"Added **{self.item_name.value}** to the shop!",
                    color=discord.Color.green()
                )
                embed.add_field(name="Price", value=f"{price} coins", inline=True)
                embed.add_field(name="Description", value=self.description.value, inline=False)
                embed.set_footer(text=f"Added by {interaction.user.display_name}")

                await interaction.response.send_message(embed=embed)

            except ValueError:
                await interaction.response.send_message("❌ Price must be a number!", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    class DeleteItemDropdown(discord.ui.Select):
        def __init__(self, shop_items, default_items):
            options = []
            self.default_items = [item.lower() for item in default_items]  # Case-insensitive Vergleich

            for item in shop_items:
                # Nur anzeigen wenn NICHT in Standard-Items
                if item['item_name'].lower() not in self.default_items:
                    options.append(discord.SelectOption(
                        label=f"{item['emoji']} {item['item_name']}",
                        description=f"{item['price']} coins",
                        value=str(item['item_id']),
                        emoji=item['emoji']
                    ))

            super().__init__(
                placeholder="Select a custom item to delete...",
                min_values=1,
                max_values=1,
                options=options or [discord.SelectOption(
                    label="No custom items available",
                    description="Only default items exist",
                    value="0"
                )],
                disabled=not options
            )

        async def callback(self, interaction: discord.Interaction):
            if self.values[0] == "0":
                await interaction.response.send_message("❌ No custom items to delete!", ephemeral=True)
                return

            try:
                item_id = int(self.values[0])
                cog = interaction.client.get_cog("EconomyCog")

                with closing(cog.get_db_connection()) as conn:
                    # Prüfe ob es ein Standard-Item ist (zusätzlicher Schutz)
                    item = conn.execute("""
                        SELECT item_name FROM shop_items 
                        WHERE item_id = ? AND LOWER(item_name) NOT IN ({})
                    """.format(','.join(['?'] * len(self.default_items))),
                                        [item_id] + self.default_items).fetchone()

                    if not item:
                        await interaction.response.send_message(
                            "❌ Cannot delete default items!",
                            ephemeral=True
                        )
                        return

                    conn.execute("DELETE FROM shop_items WHERE item_id = ?", (item_id,))
                    conn.commit()

                await interaction.response.send_message(
                    f"✅ Item successfully deleted!",
                    ephemeral=True
                )

            except Exception as e:
                await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    class DeleteItemView(discord.ui.View):
        def __init__(self, shop_items, default_items):
            super().__init__()
            self.add_item(DeleteItemDropdown(shop_items, default_items))

    class ShopConfigView(discord.ui.View):
        def __init__(self, cog, guild_id):
            super().__init__()
            self.cog = cog
            self.guild_id = guild_id

        @discord.ui.button(label="Add Item", style=discord.ButtonStyle.green, emoji="➕")
        async def add_item(self, interaction: discord.Interaction, button: discord.ui.Button):
            modal = EconomyCog.ShopItemModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)

        @discord.ui.button(label="Delete Item", style=discord.ButtonStyle.red, emoji="🗑️")
        async def delete_item(self, interaction: discord.Interaction, button: discord.ui.Button):
            with closing(self.cog.get_db_connection()) as conn:
                shop_items = conn.execute("""
                    SELECT item_id, item_name, price, emoji 
                    FROM shop_items 
                    WHERE guild_id = ?
                """, (self.guild_id,)).fetchall()

            if not shop_items:
                await interaction.response.send_message("❌ The shop is empty - nothing to delete!", ephemeral=True)
                return

            view = EconomyCog.DeleteItemView(shop_items)
            await interaction.response.send_message(
                "Select an item to delete:",
                view=view,
                ephemeral=True
            )

    @app_commands.command(name="configureshop", description="Configure shop items (Admin only)")
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    async def configure_shop(self, interaction: discord.Interaction):
        try:
            # Check permissions
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message(
                    "❌ You need 'Manage Server' permissions to use this command!",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="🛒 Shop Configuration",
                description="Manage items in the server shop",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Add Item",
                value="Click the ➕ button to add a new shop item",
                inline=False
            )
            embed.add_field(
                name="Delete Item",
                value="Click the 🗑️ button to remove an existing item",
                inline=False
            )

            view = self.ShopConfigView(self, interaction.guild.id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    class UseItemDropdown(discord.ui.Select):
        def __init__(self, user_items, cog):
            options = []
            self.cog = cog

            for item in user_items:
                try:
                    options.append(discord.SelectOption(
                        label=f"{item['emoji']} {item['item_name']} (x{item['quantity']})",
                        description=item['description'][:100],
                        value=str(item['item_id']),
                        emoji=item['emoji']
                    ))
                except Exception as e:
                    print(f"Error creating dropdown option: {e}")
                    continue

            super().__init__(
                placeholder="Select an item to use...",
                min_values=1,
                max_values=1,
                options=options or [discord.SelectOption(
                    label="No usable items",
                    description="",
                    value="0"
                )],
                disabled=not options
            )

        async def callback(self, interaction: discord.Interaction):
            if self.values[0] == "0":
                return await interaction.response.send_message(
                    "❌ No usable items selected",
                    ephemeral=True
                )

            await interaction.response.defer(ephemeral=True)
            try:
                item_id = int(self.values[0])
                success = await self.cog.use_item_by_id(interaction.user.id, item_id)

                if success:
                    await interaction.followup.send(
                        "✅ Item successfully used!",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "❌ Failed to use item. It may have expired or been used up.",
                        ephemeral=True
                    )

            except Exception as e:
                print(f"Error in dropdown callback: {e}")
                await interaction.followup.send(
                    "❌ Error using item. Please try again.",
                    ephemeral=True
                )

    class UseItemView(discord.ui.View):
        def __init__(self, user_items, cog):
            super().__init__(timeout=180)
            self.cog = cog
            self.add_item(EconomyCog.UseItemDropdown(user_items, cog))

    @app_commands.command(name="use_item", description="Use an item from your inventory")
    @app_commands.guild_only()
    async def use_item(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            current_time = int(time.time())

            try:
                user_items = await self.get_user_inventory_with_retry(interaction.user.id)
            except Exception as e:
                print(f"Database error: {e}")
                raise Exception("Failed to access inventory")

            if not user_items:
                embed = discord.Embed(
                    title="🎒 Inventory Empty",
                    description="You don't have any items to use!\nVisit `/shop` to buy some.",
                    color=discord.Color.red()
                )
                return await interaction.followup.send(embed=embed, ephemeral=True)

            # Split items into categories
            active_items = []
            usable_items = []
            admin_items = []
            default_item_names = [name.lower() for name in self.DEFAULT_SHOP_ITEMS.keys()]

            for item in user_items:
                item_name = item['item_name'].lower()
                # Für das Bankrob-Item: Falls es bereits benutzt wurde, überspringen – ansonsten bleibt es in active_items.
                if item_name == "bankrob" and item.get("used_at") is not None:
                    continue

                is_default = item_name in default_item_names
                is_active = (
                        item.get('used_at') is not None
                        and item('expires_at') is not None
                        and current_time < item['expires_at']
                )
                if is_active:
                    active_items.append(item)
                elif is_default:
                    usable_items.append(item)
                else:
                    admin_items.append(item)

            embed = discord.Embed(
                title=f"{interaction.user.display_name}'s Inventory",
                color=discord.Color.blue()
            )

            # Active Items anzeigen
            if active_items:
                active_list = []
                for item in active_items:
                    expires_at = item['expires_at']
                    time_left = self.format_time_remaining(expires_at - current_time)
                    active_list.append(
                        f"{item['emoji']} **{item['item_name']}** - "
                        f"Active until <t:{expires_at}:f> ({time_left} remaining)"
                    )
                embed.add_field(name="🟢 Active Items", value="\n".join(active_list), inline=False)
            else:
                # Zusatz: versuche aus used_items ebenfalls aktive zu holen
                used_items = []
                with closing(self.get_db_connection()) as conn:
                    rows = conn.execute("""
                        SELECT item_name, emoji, expires_at
                        FROM used_items
                        WHERE user_id = ? AND expires_at > ?
                    """, (interaction.user.id, current_time)).fetchall()

                    for row in rows:
                        time_left = self.format_time_remaining(row["expires_at"] - current_time)
                        used_items.append(
                            f"{row['emoji']} **{row['item_name']}** - Active until <t:{row['expires_at']}:f> ({time_left} remaining)"
                        )

                if used_items:
                    embed.add_field(name="🟢 Active Items", value="\n".join(used_items), inline=False)
                else:
                    embed.add_field(
                        name="⚠️ No Active Items",
                        value="You don't have any active items right now.",
                        inline=False
                    )
            # Usable Items anzeigen
            if usable_items:
                usable_list = [
                    f"{item['emoji']} {item['item_name']} (x{item['quantity']})" for item in usable_items
                ]
                embed.add_field(name="🟡 Usable Items", value="\n".join(usable_list), inline=False)

            # Admin/Special Items anzeigen
            if admin_items:
                admin_list = [
                    f"{item['emoji']} {item['item_name']} (x{item['quantity']})" for item in admin_items
                ]
                embed.add_field(
                    name=" Special Items",
                    value="\n".join(admin_list) + "\n\n*These special items cannot be activated*",
                    inline=False
                )

            # Wenn nutzbare Items vorhanden sind, zeige interaktive Auswahl
            if usable_items:
                view = self.UseItemView(usable_items, self)
                await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f"Error in use_item command: {e}")
            await interaction.followup.send("❌ Error accessing inventory. Please try again later.", ephemeral=True)

    # --- HELPER METHOD: use_item_by_id ---
    async def use_item_by_id(self, user_id: int, item_id: int) -> bool:
        """Verwendet ein Item und verschiebt es ggf. in used_items"""
        try:
            with closing(self.get_db_connection()) as conn:
                conn.execute("BEGIN IMMEDIATE TRANSACTION")

                # 1. Item holen
                item = conn.execute("""
                    SELECT ui.rowid, ui.quantity, si.item_name, si.emoji
                    FROM user_inventory ui
                    JOIN shop_items si ON ui.item_id = si.item_id
                    WHERE ui.user_id = ? AND ui.item_id = ? AND ui.quantity > 0
                """, (user_id, item_id)).fetchone()

                if not item:
                    conn.rollback()
                    return False

                item_name = item["item_name"]
                emoji = item["emoji"]
                item_data = self.DEFAULT_SHOP_ITEMS.get(item_name.lower(), {})
                duration = item_data.get("duration", 0)
                current_time = int(time.time())
                expires_at = current_time + duration if duration else None

                new_quantity = item["quantity"] - 1

                if new_quantity <= 0:
                    # Zu used_items verschieben wenn Dauer vorhanden
                    if duration:
                        conn.execute("""
                            INSERT INTO used_items (user_id, item_id, item_name, emoji, used_at, expires_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (user_id, item_id, item_name, emoji, current_time, expires_at))
                    # Immer aus user_inventory löschen
                    conn.execute("DELETE FROM user_inventory WHERE rowid = ?", (item["rowid"],))
                else:
                    conn.execute("""
                        UPDATE user_inventory 
                        SET quantity = ?, used_at = ?, expires_at = ?
                        WHERE rowid = ?
                    """, (new_quantity, current_time, expires_at, item["rowid"]))

                conn.commit()
                return True

        except sqlite3.Error as e:
            print(f"Database error in use_item_by_id: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False

    # --- HELPER METHOD: get_user_inventory_with_retry ---
    async def get_user_inventory_with_retry(self, user_id: int, max_retries: int = 3) -> list:
        """Gibt aktives Inventar inkl. Items aus used_items zurück"""
        for attempt in range(max_retries):
            try:
                with closing(self.get_db_connection()) as conn:
                    current_time = int(time.time())

                    # Normale Items
                    items = conn.execute("""
                        SELECT 
                            ui.item_id, 
                            si.item_name, 
                            si.description, 
                            si.emoji, 
                            ui.quantity,
                            ui.expires_at,
                            ui.used_at
                        FROM user_inventory ui
                        JOIN shop_items si ON ui.item_id = si.item_id
                        WHERE ui.user_id = ? 
                        AND ui.quantity > 0
                        ORDER BY si.item_name
                    """, (user_id,)).fetchall()

                    # Aktive gebrauchte Items (z.B. bankrob nach Einsatz)
                    used = conn.execute("""
                        SELECT 
                            item_id, 
                            item_name, 
                            '' AS description, 
                            emoji, 
                            1 AS quantity,
                            expires_at,
                            used_at
                        FROM used_items
                        WHERE user_id = ? AND expires_at > ?
                    """, (user_id, current_time)).fetchall()

                    # Kombinieren
                    combined = list(items) + list(used)
                    return [dict(row) for row in combined]

            except sqlite3.OperationalError as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))

        return []

    @commands.command(name="devgive", description="(Dev only) Give coins to any user")
    @commands.is_owner()
    async def devgive(self, ctx, user: discord.Member, amount: int):
        """Give coins to any user (Bot Owner Only)"""
        try:
            if amount <= 0:
                await ctx.send("❌ Amount must be positive!", delete_after=10)
                return

            if user.bot:
                await ctx.send("❌ Bots can't receive coins!", delete_after=10)
                return

            with closing(self.get_db_connection()) as conn:
                # Check if user exists
                conn.execute("""
                    INSERT OR IGNORE INTO users (user_id, username, cash, bank)
                    VALUES (?, ?, 100, 0)
                """, (user.id, str(user)))

                # Give coins
                conn.execute("""
                    UPDATE users SET 
                        cash = cash + ?,
                        total_earned = total_earned + ?
                    WHERE user_id = ?
                """, (amount, amount, user.id))
                conn.commit()

            embed = discord.Embed(
                title="✅ Dev Give Complete",
                description=f"Gave {user.mention} **{amount} coins**",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Executed by {ctx.author}")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}", delete_after=10)

    @commands.command(name="devtake")
    @commands.is_owner()
    async def devtake(self, ctx, user: discord.Member, amount: int):
        """Nimmt Geld von Cash oder Bank wenn nicht genug Cash vorhanden ist"""
        try:
            if amount <= 0:
                await ctx.send("❌ Betrag muss positiv sein!")
                return

            with closing(self.get_db_connection()) as conn:
                # Hole aktuellen Kontostand
                account = conn.execute("""
                    SELECT cash, bank FROM users WHERE user_id = ?
                """, (user.id,)).fetchone()

                if not account:
                    await ctx.send("❌ Benutzer hat kein Konto!")
                    return

                cash, bank = account['cash'], account['bank']
                total = cash + bank

                if total < amount:
                    await ctx.send(f"❌ Benutzer hat nur {total} Coins insgesamt!")
                    return

                # Zuerst von Cash abziehen
                new_cash = max(cash - amount, 0)
                remaining = amount - (cash - new_cash)

                # Falls nötig von Bank abziehen
                new_bank = max(bank - remaining, 0)

                conn.execute("""
                    UPDATE users SET 
                        cash = ?,
                        bank = ?,
                        total_spent = total_spent + ?
                    WHERE user_id = ?
                """, (new_cash, new_bank, amount, user.id))
                conn.commit()

            embed = discord.Embed(
                title="💰 Dev Take",
                description=(
                    f"Entfernt **{amount} coins** von {user.mention}\n"
                    f"Neuer Kontostand: {new_cash} 💵 (Cash) + {new_bank} 🏦 (Bank)"
                ),
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Fehler: {str(e)}")

    # --- Help Command ---
    @app_commands.command(name="economyhelp", description="Show all economy commands")
    @app_commands.guild_only()
    async def economy_help(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            embed = discord.Embed(
                title="📚 Economy Commands Guide",
                description="All available economy commands and features:",
                color=discord.Color.blurple()
            )

            # Economy Commands
            embed.add_field(
                name="💼 Money Making",
                value="""`/work` - Earn coins (1h cooldown)
    `/daily` - Daily reward (24h cooldown)
    `/stream` - Earn from streaming (2h cooldown)""",
                inline=False
            )

            # Bank System
            embed.add_field(
                name="🏦 Bank System",
                value="""`/deposit [amount|all]` - Deposit cash to bank
    `/withdraw [amount|all]` - Withdraw from bank
    `/balance` - Check your balance""",
                inline=False
            )

            # Shop System
            embed.add_field(
                name="🛒 Shop Items",
                value="""`/shop` - View available items
    `/use` - Activate purchased items
    **Available Items:**
    ☕ Coffee - XP Boost (1h)
    💻 Laptop - Double work income (24h)
    🎲 Dice - Better gambling odds (1h)
    🛡️ Shield - Theft protection (24h)
    💰 Bankrob - Steal from banks (24h)""",
                inline=False
            )

            # Gambling
            embed.add_field(
                name="🎲 Gambling (All-in available)",
                value="""`/dice [amount|all]` - Roll dice vs bot (2x payout)
    `/roulette [color] [amount|all]` - Bet on red/black (2x)
    `/slots [amount|all]` - Slot machine (2x-10x)
    `/blackjack [amount|all]` - Classic blackjack (2x-2.5x)
    `/highlow [guess] [amount|all]` - Higher/lower (2x-5x)
    **Tip:** Use 'all' to bet your entire cash!""",
                inline=False
            )

            # Social
            embed.add_field(
                name="🤝 Social",
                value="""`/give [user] [amount]` - Gift coins
    `/steal [user]` - Attempt theft (4h cooldown)
    **Protection:** 🛡️ Shield blocks theft""",
                inline=False
            )

            # Stats
            embed.add_field(
                name="📊 Statistics",
                value="""`/stats` - Detailed statistics
    `/level` - Your level progress
    `/top` - Server leaderboard""",
                inline=False
            )

            # Admin
            if interaction.user.guild_permissions.manage_guild:
                embed.add_field(
                    name="⚙️ Admin",
                    value="`/configureshop` - Manage shop items",
                    inline=False
                )

            # New Features Highlight
            embed.add_field(
                name="✨ New Features",
                value="""• **All-in betting** in all games
    • **Bank robbery** with 💰 Bankrob item
    • **Item effects** that boost your earnings
    • **Smart banking** with 'all' option""",
                inline=False
            )

            embed.set_footer(text="ℹ️ Type /help for general bot commands")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f"Help Error: {e}")
            await interaction.followup.send("❌ Error showing help", ephemeral=True)

    @commands.Cog.listener()
    async def on_ready(self):
        """Ensure default items are added when bot starts"""
        print("Bot is ready, initializing shop items...")
        self.ensure_default_shop_items()

async def setup(bot):
    cog = EconomyCog(bot)
    await bot.add_cog(cog)