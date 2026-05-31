import discord                          # Main discord.py library for interacting with the Discord API
from discord.ext import commands, tasks # commands: prefix command framework; tasks: loop scheduler
from discord import app_commands        # Slash command support
import aiosqlite                        # Async SQLite driver — lets us query the DB without blocking the event loop
import json                             # Used to parse legacy config files (channels.json, roles.json)
import os                               # Used to check if files exist before reading them
import asyncio                          # Needed to run the async main() entry point
import datetime                         # Used to generate timestamps in log messages


# Absolute path to the SQLite database file
DB_PATH = "/config/bot.db"
# Absolute path to the legacy channels config (list of channel IDs)
CONFIG_CHANNELS = "/config/channels.json"
# Absolute path to the legacy roles config (list of role names, parallel to channels)
CONFIG_ROLES = "/config/roles.json"
# Fallback channel ID used only on the very first run before the DB has a ui_channel_id entry
ROLE_UI_CHANNEL = int(os.getenv('roles_channel'))
# Path to the flag file written when legacy import fails, signals a fresh DB is needed
DB_FAIL_FILE = "/config/newDB"
# Embed color for all role UI entries
ROLE_UI_EMBED_COLOR = discord.Color.blurple()
ROLE_UI_CHUNK_SIZE = 10

# Enable all gateway intents so the bot receives full member, message, and reaction events
intents = discord.Intents.all()
# Create the bot instance with '!' as the prefix for legacy prefix commands
bot = commands.Bot(command_prefix='!', description="This is a Fic notification Bot", intents=intents)
# Global DB connection handle, assigned in init_db() before the bot starts
db: aiosqlite.Connection = None


def log(msg: str):
    # Format current time as a readable timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Print with flush=True so output appears immediately even when piped or redirected
    print(f"[{timestamp}] {msg}", flush=True)


async def write_newdb_flag(reason: str):
    # Write a plaintext flag file explaining why the legacy import failed
    try:
        with open(DB_FAIL_FILE, "w") as f:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Record timestamp and reason so the operator knows what went wrong
            f.write(f"{timestamp} — Legacy import failed: {reason}\n")
        log(f"Created newDB due to legacy import failure: {reason}")
    except Exception as e:
        # Don't crash if we can't even write the flag file — just log it
        log(f"Failed to write newDB flag file: {e}")


async def init_db():
    global db
    # Open (or create) the SQLite database at DB_PATH; connection is kept open for the bot's lifetime
    db = await aiosqlite.connect(DB_PATH)

    # roles: one row per named role; name must be unique
    await db.execute('''
    CREATE TABLE IF NOT EXISTS roles (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    ''')

    # user_roles: many-to-many mapping of Discord user IDs to internal role IDs
    await db.execute('''
    CREATE TABLE IF NOT EXISTS user_roles (
        user_id INTEGER,
        role_id INTEGER,
        PRIMARY KEY (user_id, role_id)
    )
    ''')

    # channel_roles: many-to-many mapping of Discord channel IDs to internal role IDs
    # a role can be linked to multiple channels; a channel can have multiple roles
    await db.execute('''
    CREATE TABLE IF NOT EXISTS channel_roles (
        channel_id INTEGER,
        role_id    INTEGER,
        PRIMARY KEY (channel_id, role_id)
    )
    ''')

    # settings: generic key-value store; currently holds 'ui_channel_id'
    await db.execute('''
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    )
    ''')

    # Persist all schema changes
    await db.commit()
    log("Database initialized.")


async def get_ui_channel_id():
    # Query the settings table for the current UI channel ID
    async with db.execute("SELECT value FROM settings WHERE key='ui_channel_id'") as cur:
        row = await cur.fetchone()

    # If found, cast from stored string to int and return
    if row:
        return int(row[0])

    # First run: no entry exists yet — seed it with the hardcoded fallback constant
    initial_id = ROLE_UI_CHANNEL

    # Write the initial value into the DB so future calls read from there instead of the constant
    await db.execute(
        "INSERT OR REPLACE INTO settings(key, value) VALUES('ui_channel_id', ?)",
        (str(initial_id),)
    )
    await db.commit()

    return initial_id


async def import_legacy():
    # Wrap everything in try/except so any unexpected error is caught and flagged
    try:
        # Both config files must exist; if either is missing, bail out
        if not os.path.exists(CONFIG_CHANNELS) or not os.path.exists(CONFIG_ROLES):
            log("Legacy import failed: missing config files.")
            await write_newdb_flag("Missing config files")
            return

        try:
            # Parse channels.json — expected to be a JSON array of channel ID strings
            with open(CONFIG_CHANNELS) as f:
                channels = json.load(f)
            # Parse roles.json — expected to be a JSON array of role name strings
            with open(CONFIG_ROLES) as f:
                roles = json.load(f)
        except Exception as e:
            log(f"Legacy import failed: JSON parse error: {e}")
            await write_newdb_flag(f"JSON read/parse error: {e}")
            return

        # Both files must be JSON arrays, not objects or primitives
        if not isinstance(channels, list) or not isinstance(roles, list):
            log("Legacy import failed: config files not JSON arrays.")
            await write_newdb_flag("Config files are not JSON arrays")
            return

        # Neither file can be empty
        if len(channels) == 0 or len(roles) == 0:
            log("Legacy import failed: config files empty.")
            await write_newdb_flag("Config files are empty")
            return

        # The two lists must be the same length so they can be zipped 1:1
        if len(channels) != len(roles):
            log("Legacy import failed: mismatched list lengths.")
            await write_newdb_flag("Config file lengths do not match")
            return

        # Iterate paired (channel_id, role_name) entries from the legacy config
        for ch, role_name in zip(channels, roles):
            # Insert the role name if it doesn't already exist
            await db.execute("INSERT OR IGNORE INTO roles(name) VALUES(?)", (role_name,))
            # Retrieve the auto-assigned role ID (needed for the channel_roles link)
            async with db.execute("SELECT id FROM roles WHERE name=?", (role_name,)) as cur:
                row = await cur.fetchone()
            role_id = row[0]

            # Link this role to its corresponding channel
            await db.execute(
                "INSERT OR IGNORE INTO channel_roles(channel_id, role_id) VALUES(?, ?)",
                (int(ch), role_id)
            )

        await db.commit()
        log("Legacy config imported successfully.")

    except Exception as e:
        log(f"Legacy import failed with unexpected exception: {e}")
        await write_newdb_flag(f"Unexpected exception: {e}")


async def get_role_display_name(role_id):
    # Prefer a linked channel name without trailing digits, e.g. "books"
    # over "books2" / "books3". If none exists, fall back to the first
    # valid linked channel name. If no valid linked channels exist, return None.
    async with db.execute(
        """
        SELECT channel_id
        FROM channel_roles
        WHERE role_id=?
        ORDER BY channel_id
        """,
        (role_id,)
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        return None

    channel_names = []
    for (channel_id,) in rows:
        channel_obj = bot.get_channel(channel_id)
        if channel_obj is None:
            try:
                channel_obj = await bot.fetch_channel(channel_id)
            except Exception:
                channel_obj = None

        if channel_obj is not None and getattr(channel_obj, "name", None):
            channel_names.append(channel_obj.name)

    if not channel_names:
        return None

    for name in channel_names:
        if not name[-1:].isdigit():
            return name

    return channel_names[0]

async def get_users_for_role(role_id):
    # Return a list of Discord user IDs subscribed to the given internal role ID
    async with db.execute("SELECT user_id FROM user_roles WHERE role_id=?", (role_id,)) as cur:
        return [r[0] for r in await cur.fetchall()]

async def get_role_channel_objects(role_id):
    async with db.execute(
        """
        SELECT channel_id
        FROM channel_roles
        WHERE role_id=?
        ORDER BY channel_id
        """,
        (role_id,),
    ) as cur:
        rows = await cur.fetchall()

    channels = []
    for (channel_id,) in rows:
        channel_obj = bot.get_channel(channel_id)
        if channel_obj is None:
            try:
                channel_obj = await bot.fetch_channel(channel_id)
            except Exception:
                channel_obj = None

        if channel_obj is not None:
            channels.append(channel_obj)

    return channels


async def get_role_channel_link_text(role_id):
    channels = await get_role_channel_objects(role_id)
    if not channels:
        return None
    return "\n".join(f"<#{channel.id}>" for channel in channels)

def chunk(lst, size=20):
    # Generator that yields successive slices of lst up to `size` elements each
    # Used to batch mention pings so a single message doesn't exceed Discord's mention limit
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

class RoleToggleButton(discord.ui.Button):
    def __init__(self, role_id: int):
        super().__init__(
            label="Toggle Subscription",
            style=discord.ButtonStyle.primary,
            custom_id=f"role_toggle:{role_id}",
        )
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        role_display_name = await get_role_display_name(self.role_id)
        if role_display_name is None:
            role_display_name = "Unknown Author"

        async with db.execute(
            "SELECT 1 FROM user_roles WHERE user_id=? AND role_id=?",
            (interaction.user.id, self.role_id),
        ) as cur:
            exists = await cur.fetchone()

        if exists:
            await db.execute(
                "DELETE FROM user_roles WHERE user_id=? AND role_id=?",
                (interaction.user.id, self.role_id),
            )
            await db.commit()
            log(f"User {interaction.user.id} unsubscribed from '{role_display_name}'")
            await interaction.response.send_message(
                f"You are unsubscribed from {role_display_name}",
                ephemeral=True,
            )
        else:
            await db.execute(
                "INSERT OR IGNORE INTO user_roles(user_id, role_id) "
                "VALUES(?, ?)",
                (interaction.user.id, self.role_id),
            )
            await db.commit()
            log(f"User {interaction.user.id} subscribed to '{role_display_name}'")
            await interaction.response.send_message(
                f"You are subscribed to {role_display_name}",
                ephemeral=True,
            )


class RoleContainer(discord.ui.Container):
    def __init__(self, role_id: int, display_name: str, channel_text: str):
        text = discord.ui.TextDisplay(
            f"**{display_name}**\n{channel_text}"
        )
        button_row = discord.ui.ActionRow(RoleToggleButton(role_id))

        super().__init__(
            text,
            button_row,
            accent_color=ROLE_UI_EMBED_COLOR,
        )


class RoleLayoutView(discord.ui.LayoutView):
    def __init__(self, role_entries):
        super().__init__(timeout=None)

        for role_id, display_name, channel_text in role_entries:
            self.add_item(RoleContainer(role_id, display_name, channel_text))

async def build_role_ui(ui_channel_id):
    old_channel = bot.get_channel(ui_channel_id)
    if old_channel is None:
        try:
            old_channel = await bot.fetch_channel(ui_channel_id)
        except Exception:
            old_channel = None

    if old_channel is None:
        log("[UI] UI channel not found, cannot rebuild.")
        return ui_channel_id

    name = old_channel.name
    category = old_channel.category          # None if the channel has no parent category
    guild = old_channel.guild                # Captured before deletion — needed for uncategorised recreate
    position = old_channel.position
    overwrites = old_channel.overwrites
    topic = old_channel.topic
    nsfw = old_channel.is_nsfw()
    slowmode = old_channel.slowmode_delay

    try:
        await old_channel.delete(reason="Rebuilding UI channel")
        log("[UI] Deleted old UI channel.")
    except Exception as e:
        log(f"[UI] Failed to delete UI channel: {e}")
        return ui_channel_id

    try:
        # channel.category is None when the channel sits outside any category.
        # create_text_channel lives on Category when inside one, or on Guild otherwise.
        if category is not None:
            new_channel = await category.create_text_channel(
                name=name,
                overwrites=overwrites,
                topic=topic,
                nsfw=nsfw,
                slowmode_delay=slowmode,
                reason="Recreating UI channel"
            )
        else:
            # No category — create directly on the guild
            new_channel = await guild.create_text_channel(
                name=name,
                overwrites=overwrites,
                topic=topic,
                nsfw=nsfw,
                slowmode_delay=slowmode,
                reason="Recreating UI channel"
            )
        log("[UI] Recreated UI channel.")
    except Exception as e:
        log(f"[UI] Failed to recreate UI channel: {e}")
        return ui_channel_id

    try:
        await new_channel.edit(position=position)
    except Exception as e:
        log(f"[UI] Failed to restore channel position: {e}")

    # Update DB with new channel ID
    await db.execute(
        "UPDATE settings SET value=? WHERE key='ui_channel_id'",
        (str(new_channel.id),)
    )
    await db.commit()

    async with db.execute("SELECT id FROM roles") as cur:
        rows = await cur.fetchall()

    role_entries = []
    for (role_id,) in rows:
        display_name = await get_role_display_name(role_id)
        if display_name is None:
            log(
                f"[UI] Skipping role_id {role_id}: "
                "no valid linked display channel."
            )
            continue

        channel_text = await get_role_channel_link_text(role_id)
        if channel_text is None:
            log(
                f"[UI] Skipping '{display_name}' (role_id {role_id}): "
                "no valid linked channels."
            )
            continue

        role_entries.append((display_name, role_id, channel_text))

    role_entries.sort(key=lambda x: x[0].lower())

    total_messages = 0
    for i in range(0, len(role_entries), ROLE_UI_CHUNK_SIZE):
        group = role_entries[i : i + ROLE_UI_CHUNK_SIZE]
        view_entries = [
            (role_id, display_name, channel_text)
            for display_name, role_id, channel_text in group
        ]

        try:
            await asyncio.sleep(0.1)
            view = RoleLayoutView(view_entries)
            await new_channel.send(view=view)
            total_messages += 1

            for display_name, _, _ in group:
                log(f"[UI] Added UI entry for role '{display_name}'")
        except ValueError as e:
            log(f"[UI] Failed to create V2 UI message: {e}")
        except Exception as e:
            log(f"[UI] Failed to send V2 UI message: {e}")

    log(
        f"[UI] UI rebuild complete. Posted {len(role_entries)} roles across "
        f"{total_messages} messages."
    )

    return new_channel.id

@bot.command()
@commands.has_permissions(administrator=True)  # Only admins can add roles
async def add(ctx, *, role_name):
    # Insert the role into the DB if it doesn't already exist
    await db.execute("INSERT OR IGNORE INTO roles(name) VALUES(?)", (role_name,))
    # Retrieve the role's ID (whether just inserted or pre-existing)
    async with db.execute("SELECT id FROM roles WHERE name=?", (role_name,)) as cur:
        row = await cur.fetchone()
    role_id = row[0]

    # Link the role to the channel the command was issued in
    await db.execute(
        "INSERT OR IGNORE INTO channel_roles(channel_id, role_id) VALUES(?, ?)",
        (ctx.channel.id, role_id)
    )
    await db.commit()

    log(f"Added role '{role_name}' and linked to channel {ctx.channel.id}")
    await ctx.send(f"Linked role '{role_name}' to this channel")
    # Rebuild the UI channel so the new role's card appears
    ui_channel_id = await get_ui_channel_id()
    ui_channel_id = await build_role_ui(ui_channel_id)


@bot.command()
async def ucheck(ctx, member: discord.Member):
    async with db.execute(
        """
        SELECT role_id
        FROM user_roles
        WHERE user_id = ?
        ORDER BY role_id
        """,
        (member.id,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await ctx.send(f"{member.display_name} has no roles.")
        log(f"ucheck: {member.id} has no roles.")
        return

    display_names = []
    for (role_id,) in rows:
        display_name = await get_role_display_name(role_id)
        if display_name is None:
            log(
                f"ucheck: role_id {role_id} for user {member.id} has no "
                "valid display channel."
            )
            display_name = f"Unknown Author ({role_id})"
        display_names.append(display_name)

    display_names.sort(key=str.lower)
    role_list = "\n".join(f"• {name}" for name in display_names)

    log(f"ucheck: {member.id} has roles: {', '.join(display_names)}")
    await ctx.send(f"**{member.display_name}** is subscribed to:\n{role_list}")

@bot.command()
async def rcheck(ctx, channel: discord.TextChannel):
    async with db.execute(
        """
        SELECT role_id
        FROM channel_roles
        WHERE channel_id = ?
        ORDER BY role_id
        """,
        (channel.id,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await ctx.send(f"No role is linked to {channel.mention}.")
        log(f"rcheck: no role linked to channel {channel.id}.")
        return

    guild = ctx.guild
    sections = []

    for (role_id,) in rows:
        display_name = await get_role_display_name(role_id)
        if display_name is None:
            display_name = f"Unknown Author ({role_id})"
            log(
                f"rcheck: role_id {role_id} linked to channel {channel.id} has "
                "no valid display channel."
            )

        async with db.execute(
            """
            SELECT user_id
            FROM user_roles
            WHERE role_id = ?
            """,
            (role_id,),
        ) as cur:
            user_rows = await cur.fetchall()

        if not user_rows:
            log(f'rcheck: no users for role "{display_name}".')
            sections.append(
                f"**{display_name}** — 0 subscriber(s):\n• No users subscribed"
            )
            continue

        names = []
        for (user_id,) in user_rows:
            member = guild.get_member(user_id)
            names.append(member.display_name if member else str(user_id))

        names.sort()
        log(f'rcheck: role "{display_name}" has {len(names)} subscribers.')

        section = (
            f"**{display_name}** — {len(names)} subscriber(s):\n"
            + "\n".join(f"• {n}" for n in names)
        )
        sections.append(section)

    message = "\n\n".join(sections)

    if len(message) <= 2000:
        await ctx.send(message)
    else:
        batch = ""
        for section in sections:
            addition = section + "\n\n"
            if len(batch) + len(addition) > 2000:
                if batch:
                    await ctx.send(batch.strip())
                batch = addition
            else:
                batch += addition

        if batch:
            await ctx.send(batch.strip())


@bot.tree.command(name="rcheck", description="List all users subscribed to a channel")
async def slash_rcheck(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
):
    await interaction.response.defer(ephemeral=False)

    async with db.execute(
        """
        SELECT role_id
        FROM channel_roles
        WHERE channel_id = ?
        ORDER BY role_id
        """,
        (channel.id,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await interaction.followup.send(
            f"No role is linked to {channel.mention}."
        )
        log(f"[SLASH] rcheck: no role linked to channel {channel.id}.")
        return

    guild = interaction.guild
    sections = []

    for (role_id,) in rows:
        display_name = await get_role_display_name(role_id)
        if display_name is None:
            display_name = f"Unknown Author ({role_id})"
            log(
                f"[SLASH] rcheck: role_id {role_id} linked to channel {channel.id} has no valid display channel."
            )

        async with db.execute(
            """
            SELECT user_id
            FROM user_roles
            WHERE role_id = ?
            """,
            (role_id,),
        ) as cur:
            user_rows = await cur.fetchall()

        if not user_rows:
            log(f'[SLASH] rcheck: no users for role "{display_name}".')
            sections.append(
                f"**{display_name}** — 0 subscriber(s):\n• No users subscribed"
            )
            continue

        names = []
        for (user_id,) in user_rows:
            member = guild.get_member(user_id)
            names.append(member.display_name if member else str(user_id))

        names.sort()
        log(f'[SLASH] rcheck: role "{display_name}" has {len(names)} subscribers.')

        section = (
            f"**{display_name}** — {len(names)} subscriber(s):\n"
            + "\n".join(f"• {n}" for n in names)
        )
        sections.append(section)

    message = "\n\n".join(sections)

    if len(message) <= 2000:
        await interaction.followup.send(message)
    else:
        first = True
        batch = ""

        for section in sections:
            addition = section + "\n\n"
            if len(batch) + len(addition) > 2000:
                if first:
                    await interaction.followup.send(batch.strip())
                    first = False
                else:
                    await interaction.channel.send(batch.strip())
                batch = addition
            else:
                batch += addition

        if batch:
            if first:
                await interaction.followup.send(batch.strip())
            else:
                await interaction.channel.send(batch.strip())


@bot.tree.command(name="add", description="Link a role to the current channel")
@app_commands.checks.has_permissions(administrator=True)  # Only admins can use this slash command
async def slash_add(interaction: discord.Interaction, role_name: str):
    # Defer ephemerally so the response is only visible to the invoking admin
    await interaction.response.defer(ephemeral=True)

    # Insert the role if it doesn't exist, then retrieve its ID
    await db.execute("INSERT OR IGNORE INTO roles(name) VALUES(?)", (role_name,))
    async with db.execute("SELECT id FROM roles WHERE name=?", (role_name,)) as cur:
        row = await cur.fetchone()
    role_id = row[0]

    # Link the role to the channel where the slash command was invoked
    await db.execute(
        "INSERT OR IGNORE INTO channel_roles(channel_id, role_id) VALUES(?, ?)",
        (interaction.channel.id, role_id)
    )
    await db.commit()

    log(f"[SLASH] Added role '{role_name}' and linked to channel {interaction.channel.id}")
    # Rebuild the UI so the new role card appears immediately
    ui_channel_id = await get_ui_channel_id()
    ui_channel_id = await build_role_ui(ui_channel_id)
    await interaction.followup.send(f"Linked role '{role_name}' to this channel.", ephemeral=True)


async def hard_delete_role(role_name: str):
    # Look up the role's internal ID — bail if it doesn't exist
    async with db.execute("SELECT id FROM roles WHERE name=?", (role_name,)) as cur:
        row = await cur.fetchone()

    if not row:
        return False, f"Role '{role_name}' not found in DB."

    role_id = row[0]

    await db.execute("DELETE FROM user_roles WHERE role_id=?", (role_id,))
    await db.execute("DELETE FROM channel_roles WHERE role_id=?", (role_id,))
    await db.execute("DELETE FROM roles WHERE id=?", (role_id,))
    await db.commit()

    log(f"[DELETE_ROLE] Fully deleted role '{role_name}' (role_id {role_id})")
    return True, f"Role '{role_name}' fully deleted from DB."


@bot.command(name="delete_role")
@commands.has_permissions(administrator=True)  # Only admins can delete roles
async def delete_role_prefix(ctx, *, role_name: str):
    # Fully delete the role from the DB and remove its UI message
    ok, msg = await hard_delete_role(role_name)

    await ctx.send(msg)
    if ok:
        # Rebuild the UI so the deleted role's card is removed
        ui_channel_id = await get_ui_channel_id()
        await build_role_ui(ui_channel_id)


@bot.tree.command(name="delete_role", description="Fully delete a role from the database")
@app_commands.checks.has_permissions(administrator=True)  # Only admins can use this slash command
async def delete_role_slash(interaction: discord.Interaction, role_name: str):
    # Defer ephemerally so the response is only visible to the invoking admin
    await interaction.response.defer(ephemeral=True)

    # Fully delete the role from the DB and remove its UI message
    ok, msg = await hard_delete_role(role_name)
    await interaction.followup.send(msg, ephemeral=True)

    if ok:
        # Rebuild the UI so the deleted role's card is removed
        ui_channel_id = await get_ui_channel_id()
        ui_channel_id = await build_role_ui(ui_channel_id)


@bot.command(name="rebuild_ui")
@commands.has_permissions(administrator=True)  # Only admins can trigger a UI rebuild
async def rebuild_ui_prefix(ctx):
    # Fetch the current UI channel ID and trigger a full rebuild
    ui_channel_id = await get_ui_channel_id()
    ui_channel_id = await build_role_ui(ui_channel_id)
    await ctx.send("UI rebuilt.")


@bot.tree.command(name="rebuild_ui", description="Rebuild the role UI channel")
@app_commands.checks.has_permissions(administrator=True)  # Only admins can use this slash command
async def rebuild_ui_slash(interaction: discord.Interaction):
    # Defer ephemerally so the response is only visible to the invoking admin
    await interaction.response.defer(ephemeral=True)
    # Fetch the current UI channel ID and trigger a full rebuild
    ui_channel_id = await get_ui_channel_id()
    ui_channel_id = await build_role_ui(ui_channel_id)
    await interaction.followup.send("UI rebuilt.", ephemeral=True)


@bot.tree.command(name="migrate", description="Migrate all users from Discord roles to DB and delete roles")
@app_commands.checks.has_permissions(administrator=True)
async def migrate(interaction: discord.Interaction):
    # Defer ephemerally — this can take a while on large servers
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    # Force-populate the member cache so member.roles is accurate for every member
    await guild.chunk()

    # Build a name → db_id lookup for all roles currently in the DB
    async with db.execute("SELECT id, name FROM roles") as cur:
        db_roles = {name: db_id for db_id, name in await cur.fetchall()}

    roles_to_delete = []  # Discord role objects to delete after DB writes are complete
    migrated_users = 0    # Counter for logging/reporting

    # Iterate every cached member and check their Discord roles against the DB
    # This approach is used instead of role.members because role.members is unreliable
    # even with chunking — iterating members and checking their roles is always accurate
    for member in guild.members:
        for role in member.roles:
            if role.name in db_roles:
                db_role_id = db_roles[role.name]
                # Insert the subscription — OR IGNORE prevents duplicates if run more than once
                await db.execute(
                    "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(?, ?)",
                    (member.id, db_role_id)
                )
                migrated_users += 1
                log(f"[MIGRATE] {member.id} → role '{role.name}' (db_id {db_role_id})")

    # Commit all user_role insertions in a single transaction
    await db.commit()

    # Collect the Discord role objects that need to be deleted from the server
    for role in guild.roles:
        if role.name in db_roles:
            roles_to_delete.append(role)

   
    # DRY RUN: log deletions to file instead of performing them
    deleted = 0
    with open("dry_run_deletes.log", "w") as f:
        for role in roles_to_delete:
            try:
                f.write(f"Would delete Discord role: '{role.name}' (id={role.id})\n")
                log(f"[MIGRATE][DRY RUN] Would delete role '{role.name}' (id={role.id})")
                deleted += 1
            except Exception as e:
                log(f"Failed to log role {role.name}: {e}")

    await interaction.followup.send(
        f"Migration complete. Migrated {migrated_users} user-role entries. Deleted {deleted} roles.",
        ephemeral=True
    )


@bot.tree.command(name="ucheck", description="Check which roles a user is subscribed to")
async def slash_ucheck(interaction: discord.Interaction, member: discord.Member):
    # Defer ephemerally so the role list is only visible to the person who ran the command
    await interaction.response.defer(ephemeral=True)

    # Query all roles the given member is subscribed to, sorted alphabetically
    async with db.execute(
        """
        SELECT role_id
        FROM user_roles
        WHERE user_id = ?
        ORDER BY role_id
        """,
        (member.id,)
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        log(f"[SLASH] ucheck: {member.id} has no roles.")
        await interaction.followup.send(f"{member.display_name} has no roles.", ephemeral=True)
        return

    display_names = []
    for (role_id,) in rows:
        display_name = await get_role_display_name(role_id)
        if display_name is None:
            log(
                f"[SLASH] ucheck: role_id {role_id} for user {member.id} has "
                "no valid display channel."
            )
            display_name = f"Unknown Author ({role_id})"
        display_names.append(display_name)

    display_names.sort(key=str.lower)
    role_list = "\n".join(f"• {name}" for name in display_names)

    log(f"[SLASH] ucheck: {member.id} has roles: {', '.join(display_names)}")
    await interaction.followup.send(
        f"**{member.display_name}** is subscribed to:\n{role_list}",
        ephemeral=True
    )


@bot.event
async def on_command_error(ctx, error):
    # Catch missing-permission errors for prefix commands and respond with a user-friendly message
    if isinstance(error, commands.MissingPermissions):
        log(f"Permission denied for user {ctx.author.id} on command '{ctx.command}'.")
        await ctx.send("You must be an administrator to use this command.")


@bot.tree.error
async def on_app_command_error(interaction, error):
    # Catch missing-permission errors for slash commands and respond ephemerally
    if isinstance(error, app_commands.MissingPermissions):
        log(f"[SLASH] Permission denied for user {interaction.user.id} on slash command.")
        if interaction.response.is_done():
            await interaction.followup.send(
                "You must be an administrator to use this command.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "You must be an administrator to use this command.",
                ephemeral=True,
            )

@bot.event
async def on_message(message):
    # Determine the effective channel ID — for threads/forum posts use the parent channel
    channel_id = message.channel.id
    parent = getattr(message.channel, "parent", None)
    if parent is not None:
        # Thread or forum post: use the parent channel ID for role lookups
        channel_id = parent.id

   # log(f"on_message in channel {message.channel.id} (effective {channel_id}), has_attachments={bool(message.attachments)}")

    # Look up which roles are linked to this effective channel
    async with db.execute(
        "SELECT role_id FROM channel_roles WHERE channel_id=?", (channel_id,)
    ) as cur:
        rows = await cur.fetchall()

    #log(f"Found {len(rows)} role mappings for channel_id {channel_id}")

    # Only process if the message has attachments, the channel has linked roles,
    # and the author hasn't suppressed pings with the -nopingy flag
    if message.attachments and rows and "-nopingy" not in message.content:
        file_url = message.attachments[0].url

        # Strip Discord's CDN query parameters before checking the file extension
        clean_url = file_url.split("?")[0].lower()

        # Only react to ebook/document file types the server cares about
        valid = clean_url.endswith((".epub", ".txt", ".docx", ".pdf"))

        log(f"File URL: {file_url}")
        log(f"Clean URL: {clean_url}")
        log(f"Valid extension: {valid}")

        if valid:
            log(f"Detected new file in channel {channel_id}: {file_url}")

            # Build the channel label once and repeat it in every batch message so users
            # always know which channel triggered the ghost ping
            if parent is not None:
                channel_label = f"#{parent.name} / {message.channel.name}"
            else:
                channel_label = f"#{message.channel.name}"

            # For each role linked to this channel, ping its subscribers
            for (role_id,) in rows:
                users = await get_users_for_role(role_id)

                log(f"DEBUG: get_users_for_role({role_id}) returned {len(users)} users: {users}")

                if users:
                    log(f"Pinging {len(users)} users for role_id {role_id} (message {message.id})")

                # Send mentions in batches of 20 to stay within Discord's per-message mention limit.
                # Repeat the channel name for every batch message for clarity.
                for group in chunk(users, 20):
                    mentions = " ".join(f"<@{u}>" for u in group)
                    msg = await message.channel.send(f"{channel_label}\n{mentions}")
                    # Delete the ping message after 7 seconds — it only needs to trigger notifications
                    await msg.delete(delay=7)

            # Keep the pin list from hitting Discord's 50-pin limit
            pins = await message.channel.pins()
            if len(pins) > 48:
                # Unpin the oldest message to make room
                await pins[-1].unpin()
                log("Unpinned oldest message due to pin limit.")

            # Pin the new file message so it's easy to find later
            await message.pin()
            log(f"Pinned message {message.id}")

    # Discord automatically posts a "X pinned a message" system message after every pin
    # Delete it immediately to keep the channel clean
    if message.type == discord.MessageType.pins_add:
        await message.delete()
        log("Deleted automatic pin notification.")

    # Must be called at the end of on_message so prefix commands still work
    await bot.process_commands(message)


@tasks.loop(hours=1)
async def cleanup_users():
    # Run once per hour to prune subscriptions for users who have left the server
    for guild in bot.guilds:
        # Build the set of currently present member IDs
        valid_ids = {member.id for member in guild.members}

        # Fetch all user IDs that currently have at least one subscription in the DB
        async with db.execute("SELECT user_id FROM user_roles") as cur:
            all_ids = {r[0] for r in await cur.fetchall()}

        # Any ID in the DB but not in the guild is a former member — remove their subscriptions
        invalid = all_ids - valid_ids

        for uid in invalid:
            await db.execute("DELETE FROM user_roles WHERE user_id=?", (uid,))

        await db.commit()

        if invalid:
            log(f"Cleanup removed {len(invalid)} stale user-role entries.")


@bot.event
async def on_ready():
    # Sync the slash command tree with Discord so /commands appear in the UI
    await bot.tree.sync()
    log("Bot is ready. Slash commands synced.")

    # Start the hourly cleanup loop if it isn't already running (guards against on_ready firing twice)
    if not cleanup_users.is_running():
        cleanup_users.start()
        log("Cleanup task started.")

    # Rebuild the UI channel on every startup to purge stale messages
    ui_channel_id = await get_ui_channel_id()
    ui_channel_id = await build_role_ui(ui_channel_id)
    log("Initial UI build completed.")


async def is_db_empty():
    # Returns True if no roles have been defined yet — used to decide whether to run legacy import
    try:
        async with db.execute("SELECT COUNT(*) FROM roles") as cur:
            row = await cur.fetchone()
        return row[0] == 0
    except Exception as e:
        log(f"DB check failed: {e}")
        # If the check itself fails, treat as empty so legacy import is attempted
        return True


async def main():
    # Initialize the DB schema before anything else
    await init_db()

    # If no roles exist yet, try to seed the DB from the legacy JSON config files
    if await is_db_empty():
        log("Database empty — attempting legacy import.")
        await import_legacy()

    log("Starting bot...")
    # Start the bot — this blocks until the bot is disconnected
    await bot.start(os.getenv('discord_token'))  # Replace with your token via env var or config file, never hardcode


asyncio.run(main())  # Entry point: run the async main() function using the default event loop
