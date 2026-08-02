import asyncio
import io
import os
import sys
from datetime import datetime, timedelta

import aiosqlite
import aiohttp
import discord
from discord import ui
from discord.ext import commands, tasks

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DISCORD_TOKEN    = os.environ.get("DISCORD_TOKEN")
PREFIX           = "<<"
MOD_ROLE_NAME    = os.environ.get("MOD_ROLE_NAME", "Modérateur")

LIVE_CHANNEL_ID          = int(os.environ.get("LIVE_CHANNEL_ID",          "1523496553623588935"))
PLANNING_CHANNEL_ID      = int(os.environ.get("PLANNING_CHANNEL_ID",      "1523501712306995262"))

TWITCH_CLIENT_ID         = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET     = os.environ.get("TWITCH_CLIENT_SECRET")
TWITCH_USERNAME          = os.environ.get("TWITCH_USERNAME", "exotichazle")
TWITCH_CHECK_INTERVAL    = 60

TICKET_PANEL_CHANNEL_ID  = int(os.environ.get("TICKET_PANEL_CHANNEL_ID",  "1523590910066298961"))
TICKET_CATEGORY_ID       = int(os.environ.get("TICKET_CATEGORY_ID",       "1523595618436776027"))
TICKET_DISCORD_ROLE_ID   = int(os.environ.get("TICKET_DISCORD_ROLE_ID",   "1523589244499656814"))
TICKET_TWITCH_ROLE_ID    = int(os.environ.get("TICKET_TWITCH_ROLE_ID",    "1523589375479251004"))

IMAGES_ONLY_CHANNEL_ID   = int(os.environ.get("IMAGES_ONLY_CHANNEL_ID",   "1523490263392190474"))

LOG_MESSAGES_CHANNEL_ID  = int(os.environ.get("LOG_MESSAGES_CHANNEL_ID",  "1523589795568291992"))
LOG_TICKETS_CHANNEL_ID   = int(os.environ.get("LOG_TICKETS_CHANNEL_ID",   "1523591058943119423"))
LOG_SANCTIONS_CHANNEL_ID = int(os.environ.get("LOG_SANCTIONS_CHANNEL_ID", "1523591158188474500"))
LOG_MEMBRES_CHANNEL_ID   = int(os.environ.get("LOG_MEMBRES_CHANNEL_ID",   "1523591212991516803"))

# ══════════════════════════════════════════════════════════════════════════════
# BASE DE DONNÉES
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot", "sanctions.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sanctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, user_name TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL, moderator_name TEXT NOT NULL,
                type TEXT NOT NULL, reason TEXT, duration INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                active INTEGER NOT NULL DEFAULT 1
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL UNIQUE,
                user_id INTEGER NOT NULL, user_name TEXT NOT NULL,
                guild_id INTEGER NOT NULL, type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                closed_at TEXT
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS private_voice (
                channel_id INTEGER PRIMARY KEY,
                owner_id INTEGER NOT NULL, guild_id INTEGER NOT NULL,
                mode TEXT NOT NULL DEFAULT 'public',
                panel_message_id INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS voice_whitelist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                UNIQUE(channel_id, user_id)
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS voice_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                UNIQUE(channel_id, user_id)
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS voice_saved_whitelist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL, guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                UNIQUE(owner_id, guild_id, user_id)
            )""")
        await db.commit()


# ── Sanctions ─────────────────────────────────────────────────────────────────

async def db_add_sanction(user_id, user_name, guild_id, mod_id, mod_name, stype, reason=None, duration=None):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO sanctions (user_id,user_name,guild_id,moderator_id,moderator_name,type,reason,duration,created_at,active) "
            "VALUES (?,?,?,?,?,?,?,?,datetime('now'),1)",
            (user_id, user_name, guild_id, mod_id, mod_name, stype, reason, duration))
        await db.commit()
        return cur.lastrowid

async def db_get_sanctions(guild_id, user_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if user_id:
            cur = await db.execute(
                "SELECT * FROM sanctions WHERE guild_id=? AND user_id=? ORDER BY created_at DESC",
                (guild_id, user_id))
        else:
            cur = await db.execute(
                "SELECT * FROM sanctions WHERE guild_id=? ORDER BY created_at DESC LIMIT 100",
                (guild_id,))
        return [dict(r) for r in await cur.fetchall()]

async def db_get_sanction_by_id(sid, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM sanctions WHERE id=? AND guild_id=?", (sid, guild_id))
        row = await cur.fetchone()
        return dict(row) if row else None

async def db_deactivate_sanction(sid, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sanctions SET active=0 WHERE id=? AND guild_id=?", (sid, guild_id))
        await db.commit()


# ── Tickets ───────────────────────────────────────────────────────────────────

async def db_save_ticket(channel_id, user_id, user_name, guild_id, ttype):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO tickets (channel_id,user_id,user_name,guild_id,type,status,created_at) "
            "VALUES (?,?,?,?,?,'open',datetime('now'))",
            (channel_id, user_id, user_name, guild_id, ttype))
        await db.commit()

async def db_get_ticket(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM tickets WHERE channel_id=?", (channel_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

async def db_close_ticket(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tickets SET status='closed',closed_at=datetime('now') WHERE channel_id=?",
            (channel_id,))
        await db.commit()

async def db_reopen_ticket(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tickets SET status='open',closed_at=NULL WHERE channel_id=?", (channel_id,))
        await db.commit()

async def db_count_tickets(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM tickets WHERE guild_id=?", (guild_id,))
        row = await cur.fetchone()
        return row[0] if row else 0


# ── Vocal privé ───────────────────────────────────────────────────────────────

async def db_create_voice(channel_id, owner_id, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO private_voice (channel_id,owner_id,guild_id,mode,created_at) "
            "VALUES (?,?,?,'public',datetime('now'))",
            (channel_id, owner_id, guild_id))
        await db.commit()

async def db_get_voice(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM private_voice WHERE channel_id=?", (channel_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

async def db_delete_voice(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM private_voice WHERE channel_id=?", (channel_id,))
        await db.execute("DELETE FROM voice_whitelist WHERE channel_id=?", (channel_id,))
        await db.execute("DELETE FROM voice_blacklist WHERE channel_id=?", (channel_id,))
        await db.commit()

async def db_set_voice_mode(channel_id, mode):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE private_voice SET mode=? WHERE channel_id=?", (mode, channel_id))
        await db.commit()

async def db_set_voice_owner(channel_id, owner_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE private_voice SET owner_id=? WHERE channel_id=?", (owner_id, channel_id))
        await db.commit()

async def db_set_voice_panel(channel_id, message_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE private_voice SET panel_message_id=? WHERE channel_id=?",
                         (message_id, channel_id))
        await db.commit()

async def db_add_wl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO voice_whitelist (channel_id,user_id) VALUES (?,?)",
                         (channel_id, user_id))
        await db.commit()

async def db_rm_wl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM voice_whitelist WHERE channel_id=? AND user_id=?",
                         (channel_id, user_id))
        await db.commit()

async def db_clear_wl(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM voice_whitelist WHERE channel_id=?", (channel_id,))
        await db.commit()

async def db_get_wl(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM voice_whitelist WHERE channel_id=?", (channel_id,))
        return [r[0] for r in await cur.fetchall()]

async def db_in_wl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM voice_whitelist WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        return await cur.fetchone() is not None

async def db_add_bl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO voice_blacklist (channel_id,user_id) VALUES (?,?)",
                         (channel_id, user_id))
        await db.commit()

async def db_rm_bl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM voice_blacklist WHERE channel_id=? AND user_id=?",
                         (channel_id, user_id))
        await db.commit()

async def db_get_bl(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM voice_blacklist WHERE channel_id=?", (channel_id,))
        return [r[0] for r in await cur.fetchall()]

async def db_in_bl(channel_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM voice_blacklist WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        return await cur.fetchone() is not None

async def db_save_wl_snapshot(owner_id, guild_id, user_ids):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM voice_saved_whitelist WHERE owner_id=? AND guild_id=?", (owner_id, guild_id))
        for uid in user_ids:
            await db.execute(
                "INSERT OR IGNORE INTO voice_saved_whitelist (owner_id,guild_id,user_id) VALUES (?,?,?)",
                (owner_id, guild_id, uid))
        await db.commit()

async def db_load_wl_snapshot(owner_id, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM voice_saved_whitelist WHERE owner_id=? AND guild_id=?",
            (owner_id, guild_id))
        return [r[0] for r in await cur.fetchall()]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS COMMUNS
# ══════════════════════════════════════════════════════════════════════════════

SANCTION_COLORS = {
    "ban": discord.Color.red(), "kick": discord.Color.orange(),
    "timeout": discord.Color.gold(), "warn": discord.Color.yellow(),
    "unban": discord.Color.green(), "untimeout": discord.Color.green(),
    "delsanction": discord.Color.green(),
}
SANCTION_LABELS = {
    "ban": "🔨 Bannissement", "kick": "👢 Expulsion",
    "timeout": "⏱️ Timeout", "warn": "⚠️ Avertissement",
    "unban": "✅ Déban", "untimeout": "✅ Timeout levé",
    "delsanction": "🗑️ Sanction supprimée",
}


async def send_sanction_log(guild, stype, member, moderator, reason, sid, duration=None):
    ch = guild.get_channel(LOG_SANCTIONS_CHANNEL_ID)
    if not ch:
        return
    embed = discord.Embed(
        title=SANCTION_LABELS.get(stype, stype),
        color=SANCTION_COLORS.get(stype, discord.Color.greyple()),
        timestamp=datetime.utcnow(),
    )
    if hasattr(member, "mention"):
        embed.add_field(name="Membre", value=f"{member.mention} (`{member}`)", inline=True)
    else:
        embed.add_field(name="Membre", value=str(member), inline=True)
    embed.add_field(name="Modérateur", value=moderator.mention, inline=True)
    embed.add_field(name="Raison", value=reason or "Aucune", inline=False)
    if duration:
        embed.add_field(name="Durée", value=f"{duration} minute(s)", inline=True)
    embed.add_field(name="ID Sanction", value=f"#{sid}", inline=True)
    if hasattr(member, "display_avatar"):
        embed.set_thumbnail(url=member.display_avatar.url)
    await ch.send(embed=embed)


async def send_sanction_dm(member, stype, reason, guild_name, sid, duration=None):
    try:
        embed = discord.Embed(
            title=f"{SANCTION_LABELS.get(stype, stype)} sur **{guild_name}**",
            color=SANCTION_COLORS.get(stype, discord.Color.greyple()),
        )
        embed.add_field(name="Raison", value=reason or "Aucune raison fournie", inline=False)
        if duration and stype == "timeout":
            embed.add_field(name="Durée", value=f"{duration} minute(s)", inline=False)
        embed.add_field(name="Référence sanction", value=f"#{sid}", inline=False)
        embed.set_footer(text=f"Serveur : {guild_name}")
        await member.send(embed=embed)
    except discord.Forbidden:
        pass


def has_mod_role():
    async def predicate(ctx):
        role = discord.utils.get(ctx.guild.roles, name=MOD_ROLE_NAME)
        if role and role in ctx.author.roles:
            return True
        if await ctx.bot.is_owner(ctx.author):
            return True
        raise commands.MissingRole(MOD_ROLE_NAME)
    return commands.check(predicate)


# ══════════════════════════════════════════════════════════════════════════════
# TICKETS — VIEWS
# ══════════════════════════════════════════════════════════════════════════════

TICKET_TYPES = {
    "discord": {
        "label": "Discord", "emoji": "💬",
        "color": discord.Color.blurple(), "role_id": TICKET_DISCORD_ROLE_ID,
        "description": "Problème ou question concernant le serveur Discord",
    },
    "twitch": {
        "label": "Twitch", "emoji": "🟣",
        "color": discord.Color.purple(), "role_id": TICKET_TWITCH_ROLE_ID,
        "description": "Problème ou question concernant le Twitch",
    },
}


async def generate_transcript(channel: discord.TextChannel) -> io.BytesIO:
    lines = [
        f"=== TRANSCRIPT — #{channel.name} ===",
        f"Généré le : {datetime.utcnow().strftime('%d/%m/%Y à %H:%M')} UTC",
        "=" * 50, "",
    ]
    async for msg in channel.history(limit=None, oldest_first=True):
        ts = msg.created_at.strftime("%d/%m/%Y %H:%M")
        lines.append(f"[{ts}] {msg.author} : {msg.content or '[message sans texte]'}")
        for a in msg.attachments:
            lines.append(f"  📎 {a.url}")
        for e in msg.embeds:
            if e.title:
                lines.append(f"  📋 Embed : {e.title}")
    lines += ["", "=" * 50, "Fin du transcript."]
    return io.BytesIO("\n".join(lines).encode("utf-8"))


async def send_ticket_log(guild, action, actor, channel, ttype, tid):
    ch = guild.get_channel(LOG_TICKETS_CHANNEL_ID)
    if not ch:
        return
    info = TICKET_TYPES.get(ttype, {})
    embed = discord.Embed(
        title=f"{info.get('emoji','🎫')} Ticket #{tid:04d} {'ouvert' if action=='open' else 'fermé'}",
        color=discord.Color.green() if action == "open" else discord.Color.red(),
        timestamp=datetime.utcnow(),
    )
    embed.add_field(name="Salon", value=channel.mention, inline=True)
    embed.add_field(name="Type", value=ttype.title(), inline=True)
    embed.add_field(name="Par", value=actor.mention, inline=True)
    embed.set_thumbnail(url=actor.display_avatar.url)
    await ch.send(embed=embed)


class TicketPanelView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="🎫 Créer un ticket", style=discord.ButtonStyle.primary,
               custom_id="persistent:ticket_create")
    async def create_ticket(self, interaction: discord.Interaction, button: ui.Button):
        async with aiosqlite.connect(DB_PATH) as conn:
            cur = await conn.execute(
                "SELECT channel_id FROM tickets WHERE user_id=? AND guild_id=? AND status='open'",
                (interaction.user.id, interaction.guild.id))
            existing = await cur.fetchone()
        if existing:
            ch = interaction.guild.get_channel(existing[0])
            if ch:
                return await interaction.response.send_message(
                    f"❌ Tu as déjà un ticket ouvert : {ch.mention}", ephemeral=True)
        await interaction.response.send_message(
            "Quel type de ticket souhaites-tu ouvrir ?", view=TicketTypeView(), ephemeral=True)


class TicketTypeView(ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    async def _open(self, interaction: discord.Interaction, ttype: str):
        await interaction.response.defer(ephemeral=True)
        info = TICKET_TYPES[ttype]
        guild, user = interaction.guild, interaction.user

        async with aiosqlite.connect(DB_PATH) as conn:
            cur = await conn.execute(
                "SELECT channel_id FROM tickets WHERE user_id=? AND guild_id=? AND status='open'",
                (user.id, guild.id))
            existing = await cur.fetchone()
        if existing:
            ch = guild.get_channel(existing[0])
            msg = f"❌ Tu as déjà un ticket ouvert : {ch.mention}" if ch else "❌ Tu as déjà un ticket ouvert."
            return await interaction.followup.send(msg, ephemeral=True)

        mod_role = discord.utils.get(guild.roles, name=MOD_ROLE_NAME)
        ping_role = guild.get_role(info["role_id"])
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, manage_channels=True,
                attach_files=True, embed_links=True, read_message_history=True),
            user: discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                attach_files=True, read_message_history=True),
        }
        if mod_role:
            overwrites[mod_role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                attach_files=True, read_message_history=True, manage_messages=True)

        category = guild.get_channel(TICKET_CATEGORY_ID)
        ticket_num = await db_count_tickets(guild.id) + 1
        channel_name = f"ticket-{user.name.lower().replace(' ', '-')}-{ticket_num:04d}"
        channel = await guild.create_text_channel(
            channel_name, overwrites=overwrites, category=category,
            reason=f"Ticket {ttype} ouvert par {user}")

        await db_save_ticket(channel.id, user.id, str(user), guild.id, ttype)

        embed = discord.Embed(
            title=f"{info['emoji']} Ticket {info['label']} #{ticket_num:04d}",
            description=(
                f"Bonjour {user.mention} !\n\n"
                f"Ton ticket **{info['label']}** a bien été créé.\n"
                "Décris ton problème ou ta question, l'équipe te répondra rapidement.\n\n"
                "Clique sur **🔒 Fermer le ticket** quand ton problème est résolu."
            ),
            color=info["color"], timestamp=datetime.utcnow(),
        )
        embed.set_footer(text=f"Ticket #{ticket_num:04d} — {user}")
        await channel.send(
            content=f"{user.mention} {ping_role.mention if ping_role else ''}",
            embed=embed, view=TicketCloseView())
        await send_ticket_log(guild, "open", user, channel, ttype, ticket_num)
        await interaction.followup.send(f"✅ Ton ticket a été créé : {channel.mention}", ephemeral=True)

    @ui.button(label="💬 Discord", style=discord.ButtonStyle.blurple)
    async def discord_btn(self, interaction, button):
        await self._open(interaction, "discord")

    @ui.button(label="🟣 Twitch", style=discord.ButtonStyle.secondary)
    async def twitch_btn(self, interaction, button):
        await self._open(interaction, "twitch")


class TicketCloseView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="🔒 Fermer le ticket", style=discord.ButtonStyle.danger,
               custom_id="persistent:ticket_close")
    async def close_btn(self, interaction: discord.Interaction, button: ui.Button):
        ticket = await db_get_ticket(interaction.channel.id)
        if not ticket:
            return await interaction.response.send_message(
                "❌ Ce salon n'est pas un ticket.", ephemeral=True)
        if ticket["status"] == "closed":
            return await interaction.response.send_message(
                "⚠️ Ce ticket est déjà fermé.", ephemeral=True)

        mod_role = discord.utils.get(interaction.guild.roles, name=MOD_ROLE_NAME)
        is_owner = interaction.user.id == ticket["user_id"]
        is_mod = mod_role and mod_role in interaction.user.roles
        is_bot_owner = await interaction.client.is_owner(interaction.user)
        if not (is_owner or is_mod or is_bot_owner):
            return await interaction.response.send_message(
                "❌ Seul le créateur du ticket ou un modérateur peut le fermer.", ephemeral=True)

        await db_close_ticket(interaction.channel.id)

        safe = "".join(
            c for c in ticket["user_name"].split("#")[0].lower().replace(" ", "-")
            if c.isalnum() or c == "-") or "user"
        try:
            await interaction.channel.edit(name=f"closed-{safe}-{ticket['id']:04d}")
        except discord.HTTPException:
            pass

        ticket_user = interaction.guild.get_member(ticket["user_id"])
        if ticket_user:
            try:
                await interaction.channel.set_permissions(
                    ticket_user, send_messages=False, view_channel=True, read_message_history=True)
            except discord.HTTPException:
                pass

        await send_ticket_log(
            interaction.guild, "close", interaction.user,
            interaction.channel, ticket["type"], ticket["id"])

        embed = discord.Embed(
            title="🔒 Ticket fermé",
            description=f"Ticket fermé par {interaction.user.mention}.\n\nChoisis une action ci-dessous :",
            color=discord.Color.dark_grey(), timestamp=datetime.utcnow(),
        )
        await interaction.response.send_message(embed=embed, view=TicketActionView())


class TicketActionView(ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @ui.button(label="📄 Transcript + Supprimer", style=discord.ButtonStyle.primary)
    async def transcript_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        channel = interaction.channel
        ticket = await db_get_ticket(channel.id)
        transcript = await generate_transcript(channel)
        log_ch = interaction.guild.get_channel(LOG_TICKETS_CHANNEL_ID)
        if log_ch:
            embed = discord.Embed(
                title="📄 Transcript de ticket",
                description=(
                    f"**Salon :** #{channel.name}\n"
                    f"**Type :** {ticket['type'].title() if ticket else '?'}\n"
                    f"**Créé par :** <@{ticket['user_id']}>" if ticket else "?\n"
                    f"**Fermé par :** {interaction.user.mention}"
                ),
                color=discord.Color.blurple(), timestamp=datetime.utcnow(),
            )
            await log_ch.send(
                embed=embed,
                file=discord.File(transcript, filename=f"transcript-{channel.name}.txt"))
        await interaction.followup.send("✅ Transcript envoyé dans les logs. Suppression dans 5 secondes…")
        await asyncio.sleep(5)
        try:
            await channel.delete(reason="Ticket clôturé avec transcript")
        except discord.HTTPException:
            pass

    @ui.button(label="🗑️ Supprimer sans transcript", style=discord.ButtonStyle.danger)
    async def delete_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message("🗑️ Suppression dans 5 secondes…")
        await asyncio.sleep(5)
        try:
            await interaction.channel.delete(reason="Ticket clôturé sans transcript")
        except discord.HTTPException:
            pass

    @ui.button(label="↩️ Réouvrir", style=discord.ButtonStyle.secondary)
    async def reopen_btn(self, interaction: discord.Interaction, button: ui.Button):
        mod_role = discord.utils.get(interaction.guild.roles, name=MOD_ROLE_NAME)
        is_mod = mod_role and mod_role in interaction.user.roles
        is_bot_owner = await interaction.client.is_owner(interaction.user)
        if not (is_mod or is_bot_owner):
            return await interaction.response.send_message(
                "❌ Seul un modérateur peut réouvrir un ticket.", ephemeral=True)
        ticket = await db_get_ticket(interaction.channel.id)
        if ticket:
            await db_reopen_ticket(interaction.channel.id)
            ticket_user = interaction.guild.get_member(ticket["user_id"])
            if ticket_user:
                try:
                    await interaction.channel.set_permissions(
                        ticket_user, send_messages=True, view_channel=True, read_message_history=True)
                except discord.HTTPException:
                    pass
        await interaction.response.send_message(
            f"✅ Ticket réouvert par {interaction.user.mention}.", view=TicketCloseView())
        self.stop()


# ══════════════════════════════════════════════════════════════════════════════
# VOCAL PRIVÉ — UTILITAIRES & VIEWS
# ══════════════════════════════════════════════════════════════════════════════

async def build_voice_embed(channel_id: int, guild: discord.Guild) -> discord.Embed:
    info = await db_get_voice(channel_id)
    if not info:
        return discord.Embed(title="❌ Salon introuvable", color=discord.Color.red())
    owner = guild.get_member(info["owner_id"])
    wl_ids = await db_get_wl(channel_id)
    bl_ids = await db_get_bl(channel_id)
    mode_labels = {"public": "🌐 Public", "private": "🔒 Privé", "waiting": "⏳ Privé avec attente"}
    mode_desc = {
        "public": "Tout le monde peut rejoindre.",
        "private": "Seuls le propriétaire et la whitelist peuvent rejoindre.",
        "waiting": "La whitelist rejoint directement. Les autres passent par la salle d'attente.",
    }
    embed = discord.Embed(
        title="🎙️ Panneau de contrôle — Salon privé",
        color=discord.Color.blurple(), timestamp=datetime.utcnow())
    embed.add_field(name="👑 Propriétaire",
                    value=owner.mention if owner else f"<@{info['owner_id']}>", inline=True)
    embed.add_field(name="📡 Mode", value=mode_labels.get(info["mode"], info["mode"]), inline=True)
    wl_text = " ".join(f"<@{u}>" for u in wl_ids) if wl_ids else "*(vide)*"
    bl_text = " ".join(f"<@{u}>" for u in bl_ids) if bl_ids else "*(vide)*"
    embed.add_field(name=f"✅ Whitelist ({len(wl_ids)})", value=wl_text[:1024], inline=False)
    embed.add_field(name=f"🚫 Blacklist ({len(bl_ids)})", value=bl_text[:1024], inline=False)
    embed.set_footer(text=mode_desc.get(info["mode"], ""))
    return embed


async def refresh_voice_panel(channel: discord.VoiceChannel, channel_id: int):
    info = await db_get_voice(channel_id)
    if not info or not info.get("panel_message_id"):
        return
    try:
        msg = await channel.fetch_message(info["panel_message_id"])
        embed = await build_voice_embed(channel_id, channel.guild)
        await msg.edit(embed=embed)
    except discord.HTTPException:
        pass


async def sync_voice_permissions(vc: discord.VoiceChannel, guild: discord.Guild):
    info = await db_get_voice(vc.id)
    if not info:
        return
    if info["mode"] == "public":
        try:
            await vc.edit(overwrites={})
        except discord.HTTPException:
            pass
        return
    wl_ids = await db_get_wl(vc.id)
    overwrites: dict = {guild.default_role: discord.PermissionOverwrite(connect=False)}
    owner = guild.get_member(info["owner_id"])
    if owner:
        overwrites[owner] = discord.PermissionOverwrite(connect=True)
    for uid in wl_ids:
        m = guild.get_member(uid)
        if m and m not in overwrites:
            overwrites[m] = discord.PermissionOverwrite(connect=True)
    try:
        await vc.edit(overwrites=overwrites)
    except discord.HTTPException:
        pass


async def get_or_create_waiting_channel(guild, private_channel, cog):
    ch_id = private_channel.id
    wait_id = cog._waiting_channels.get(ch_id)
    if wait_id:
        wc = guild.get_channel(wait_id)
        if wc:
            return wc
    wait_name = f"⏳ {private_channel.name}"
    wc = discord.utils.get(guild.voice_channels, name=wait_name, category=private_channel.category)
    if not wc:
        try:
            wc = await guild.create_voice_channel(
                name=wait_name, category=private_channel.category, reason="Salle d'attente")
        except discord.HTTPException:
            return None
    cog._waiting_channels[ch_id] = wc.id
    return wc


async def delete_waiting_channel(guild, channel_id, cog):
    wait_id = cog._waiting_channels.pop(channel_id, None)
    if wait_id:
        wc = guild.get_channel(wait_id)
        if wc:
            for m in list(wc.members):
                try:
                    await m.move_to(None)
                except discord.HTTPException:
                    pass
            try:
                await wc.delete(reason="Mode attente désactivé")
            except discord.HTTPException:
                pass


# ── Sélection whitelist ────────────────────────────────────────────────────────

class AddWhitelistView(ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=120)
        self.channel_id = channel_id

    @ui.select(cls=ui.MentionableSelect, placeholder="Rôles et membres de la whitelist",
               min_values=0, max_values=25, row=0)
    async def select(self, interaction: discord.Interaction, select: ui.MentionableSelect):
        added = 0
        for item in select.values:
            if isinstance(item, discord.Member):
                await db_add_wl(self.channel_id, item.id); added += 1
            elif isinstance(item, discord.Role):
                for m in item.members:
                    if not m.bot:
                        await db_add_wl(self.channel_id, m.id); added += 1
        await interaction.response.send_message(
            f"✅ {added} membre(s) ajouté(s) à la whitelist.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
            await refresh_voice_panel(vc, self.channel_id)

    @ui.button(label="Tout le salon", style=discord.ButtonStyle.primary, row=1)
    async def all_members(self, interaction: discord.Interaction, button: ui.Button):
        count = 0
        for m in interaction.guild.members:
            if not m.bot:
                await db_add_wl(self.channel_id, m.id)
                count += 1
        await interaction.response.send_message(
            f"✅ {count} membre(s) du salon ajouté(s) à la whitelist.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
            await refresh_voice_panel(vc, self.channel_id)

    @ui.button(label="Remettre à zéro", style=discord.ButtonStyle.danger, row=1)
    async def reset(self, interaction: discord.Interaction, button: ui.Button):
        await db_clear_wl(self.channel_id)
        await interaction.response.send_message("🗑️ Whitelist remise à zéro.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
            await refresh_voice_panel(vc, self.channel_id)


class RemoveWhitelistView(ui.View):
    def __init__(self, channel_id: int, options: list):
        super().__init__(timeout=120)
        self.channel_id = channel_id
        select = ui.Select(placeholder="Choisir les membres à retirer…",
                           options=options, min_values=0, max_values=len(options), row=0)
        select.callback = self._on_select
        self.add_item(select)

    async def _on_select(self, interaction: discord.Interaction):
        select: ui.Select = self.children[0]
        for uid_str in select.values:
            await db_rm_wl(self.channel_id, int(uid_str))
        await interaction.response.send_message(
            f"❌ {len(select.values)} membre(s) retiré(s) de la whitelist.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
            await refresh_voice_panel(vc, self.channel_id)


class AddBlacklistView(ui.View):
    def __init__(self, channel_id: int, private_channel):
        super().__init__(timeout=120)
        self.channel_id = channel_id
        self.private_channel = private_channel

    @ui.select(cls=ui.UserSelect, placeholder="Sélectionne des membres à blacklister…",
               min_values=0, max_values=25, row=0)
    async def select(self, interaction: discord.Interaction, select: ui.UserSelect):
        count = 0
        for member in select.values:
            if not member.bot:
                await db_add_bl(self.channel_id, member.id)
                if member in self.private_channel.members:
                    try:
                        await member.move_to(None)
                    except discord.HTTPException:
                        pass
                count += 1
        await interaction.response.send_message(
            f"🚫 {count} membre(s) ajouté(s) à la blacklist.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await refresh_voice_panel(vc, self.channel_id)


class RemoveBlacklistView(ui.View):
    def __init__(self, channel_id: int, options: list):
        super().__init__(timeout=120)
        self.channel_id = channel_id
        select = ui.Select(placeholder="Choisir les membres à retirer de la blacklist…",
                           options=options, min_values=0, max_values=len(options), row=0)
        select.callback = self._on_select
        self.add_item(select)

    async def _on_select(self, interaction: discord.Interaction):
        select: ui.Select = self.children[0]
        for uid_str in select.values:
            await db_rm_bl(self.channel_id, int(uid_str))
        await interaction.response.send_message(
            f"🔓 {len(select.values)} membre(s) retiré(s) de la blacklist.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await refresh_voice_panel(vc, self.channel_id)


class ChangeOwnerView(ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=120)
        self.channel_id = channel_id

    @ui.select(cls=ui.UserSelect, placeholder="Sélectionne le nouveau propriétaire…",
               min_values=1, max_values=1, row=0)
    async def select(self, interaction: discord.Interaction, select: ui.UserSelect):
        new_owner: discord.Member = select.values[0]
        if new_owner.bot:
            return await interaction.response.send_message(
                "❌ Tu ne peux pas donner la propriété à un bot.", ephemeral=True)
        await db_set_voice_owner(self.channel_id, new_owner.id)
        await interaction.response.send_message(
            f"👑 **{new_owner.display_name}** est maintenant propriétaire du salon.", ephemeral=True)
        vc = interaction.guild.get_channel(self.channel_id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
            await refresh_voice_panel(vc, self.channel_id)


# ── Panneau de contrôle (persistant) ──────────────────────────────────────────

class VoiceControlPanel(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _check_owner(self, interaction: discord.Interaction) -> bool:
        info = await db_get_voice(interaction.channel.id)
        if not info:
            await interaction.response.send_message("❌ Ce panneau n'est plus actif.", ephemeral=True)
            return False
        if interaction.user.id != info["owner_id"] and not await interaction.client.is_owner(interaction.user):
            await interaction.response.send_message(
                "❌ Seul le propriétaire peut modifier ces paramètres.", ephemeral=True)
            return False
        return True

    def _get_cog(self, interaction):
        return interaction.client.get_cog("VoicePrivate")

    # Modes
    @ui.button(label="🌐 Public", style=discord.ButtonStyle.secondary, custom_id="vpc:mode:public", row=0)
    async def mode_public(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        ch_id = interaction.channel.id
        cog = self._get_cog(interaction)
        if cog: await delete_waiting_channel(interaction.guild, ch_id, cog)
        await db_set_voice_mode(ch_id, "public")
        await sync_voice_permissions(interaction.channel, interaction.guild)
        await interaction.response.send_message("🌐 Mode **Public** activé.", ephemeral=True)
        await refresh_voice_panel(interaction.channel, ch_id)

    @ui.button(label="🔒 Privé", style=discord.ButtonStyle.secondary, custom_id="vpc:mode:private", row=0)
    async def mode_private(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        ch_id = interaction.channel.id
        cog = self._get_cog(interaction)
        if cog: await delete_waiting_channel(interaction.guild, ch_id, cog)
        await db_set_voice_mode(ch_id, "private")
        await sync_voice_permissions(interaction.channel, interaction.guild)
        await interaction.response.send_message("🔒 Mode **Privé** activé.", ephemeral=True)
        await refresh_voice_panel(interaction.channel, ch_id)

    @ui.button(label="⏳ Avec attente", style=discord.ButtonStyle.secondary, custom_id="vpc:mode:waiting", row=0)
    async def mode_waiting(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        ch_id = interaction.channel.id
        await db_set_voice_mode(ch_id, "waiting")
        cog = self._get_cog(interaction)
        vc = interaction.guild.get_channel(ch_id)
        if cog and vc: await get_or_create_waiting_channel(interaction.guild, vc, cog)
        if vc: await sync_voice_permissions(vc, interaction.guild)
        await interaction.response.send_message(
            "⏳ Mode **Privé avec attente** activé. La salle d'attente a été créée.", ephemeral=True)
        await refresh_voice_panel(interaction.channel, ch_id)

    # Whitelist
    @ui.button(label="✅ Add whitelist", style=discord.ButtonStyle.success, custom_id="vpc:wl:add", row=1)
    async def wl_add(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        await interaction.response.send_message(
            "Sélectionne les membres ou rôles à ajouter à la whitelist :",
            view=AddWhitelistView(interaction.channel.id), ephemeral=True)

    @ui.button(label="❌ Retirer whitelist", style=discord.ButtonStyle.danger, custom_id="vpc:wl:rm", row=1)
    async def wl_rm(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        ch_id = interaction.channel.id
        wl_ids = await db_get_wl(ch_id)
        if not wl_ids:
            return await interaction.response.send_message("ℹ️ La whitelist est déjà vide.", ephemeral=True)
        options = []
        for uid in wl_ids[:25]:
            m = interaction.guild.get_member(uid)
            options.append(discord.SelectOption(label=m.display_name if m else f"User {uid}",
                                                value=str(uid), emoji="✅" if m else None))
        await interaction.response.send_message(
            "Sélectionne les membres à retirer de la whitelist :",
            view=RemoveWhitelistView(ch_id, options), ephemeral=True)

    # Blacklist
    @ui.button(label="🚫 Add blacklist", style=discord.ButtonStyle.danger, custom_id="vpc:bl:add", row=1)
    async def bl_add(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        vc = interaction.guild.get_channel(interaction.channel.id)
        await interaction.response.send_message(
            "Sélectionne les membres à ajouter à la blacklist :",
            view=AddBlacklistView(interaction.channel.id, vc), ephemeral=True)

    @ui.button(label="🔓 Retirer blacklist", style=discord.ButtonStyle.secondary, custom_id="vpc:bl:rm", row=1)
    async def bl_rm(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        ch_id = interaction.channel.id
        bl_ids = await db_get_bl(ch_id)
        if not bl_ids:
            return await interaction.response.send_message("ℹ️ La blacklist est déjà vide.", ephemeral=True)
        options = []
        for uid in bl_ids[:25]:
            m = interaction.guild.get_member(uid)
            options.append(discord.SelectOption(label=m.display_name if m else f"User {uid}",
                                                value=str(uid), emoji="🚫" if m else None))
        await interaction.response.send_message(
            "Sélectionne les membres à retirer de la blacklist :",
            view=RemoveBlacklistView(ch_id, options), ephemeral=True)

    # Owner & Sauvegarde
    @ui.button(label="👑 Changer owner", style=discord.ButtonStyle.primary, custom_id="vpc:owner", row=2)
    async def set_owner(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        await interaction.response.send_message(
            "Sélectionne le nouveau propriétaire du salon :",
            view=ChangeOwnerView(interaction.channel.id), ephemeral=True)

    @ui.button(label="💾 Sauvegarder WL", style=discord.ButtonStyle.secondary, custom_id="vpc:save_wl", row=2)
    async def save_wl(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        info = await db_get_voice(interaction.channel.id)
        wl = await db_get_wl(interaction.channel.id)
        await db_save_wl_snapshot(info["owner_id"], interaction.guild.id, wl)
        await interaction.response.send_message(
            f"💾 Whitelist sauvegardée ({len(wl)} membre(s)).", ephemeral=True)

    @ui.button(label="📂 Charger WL", style=discord.ButtonStyle.secondary, custom_id="vpc:load_wl", row=2)
    async def load_wl(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._check_owner(interaction): return
        info = await db_get_voice(interaction.channel.id)
        saved = await db_load_wl_snapshot(info["owner_id"], interaction.guild.id)
        if not saved:
            return await interaction.response.send_message("📂 Aucune whitelist sauvegardée.", ephemeral=True)
        await db_clear_wl(interaction.channel.id)
        for uid in saved:
            await db_add_wl(interaction.channel.id, uid)
        await interaction.response.send_message(
            f"📂 Whitelist chargée ({len(saved)} membre(s)).", ephemeral=True)
        vc = interaction.guild.get_channel(interaction.channel.id)
        if vc:
            await sync_voice_permissions(vc, interaction.guild)
        await refresh_voice_panel(interaction.channel, interaction.channel.id)


# ── Approbation salle d'attente ────────────────────────────────────────────────

class WaitingApprovalView(ui.View):
    def __init__(self, channel_id: int, waiting_member: discord.Member, waiting_channel):
        super().__init__(timeout=300)
        self.channel_id = channel_id
        self.waiting_member = waiting_member
        self.waiting_channel = waiting_channel
        self.handled = False

    async def _only_owner(self, interaction: discord.Interaction) -> bool:
        info = await db_get_voice(self.channel_id)
        if not info or interaction.user.id != info["owner_id"]:
            await interaction.response.send_message(
                "❌ Seul le propriétaire peut accepter ou refuser.", ephemeral=True)
            return False
        return True

    @ui.button(label="✅ Accepter", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: ui.Button):
        if self.handled:
            return await interaction.response.send_message("⚠️ Déjà traité.", ephemeral=True)
        if not await self._only_owner(interaction): return
        self.handled = True; self.stop()
        target = interaction.guild.get_channel(self.channel_id)
        if not target:
            return await interaction.response.send_message("❌ Le salon vocal n'existe plus.", ephemeral=True)
        cog = interaction.client.get_cog("VoicePrivate")
        if cog:
            cog._being_accepted.add(self.waiting_member.id)
        if self.waiting_member in self.waiting_channel.members:
            try:
                await self.waiting_member.move_to(target)
            except discord.HTTPException:
                pass
            await interaction.response.send_message(
                f"✅ **{self.waiting_member.display_name}** a été déplacé dans ton salon.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"⚠️ **{self.waiting_member.display_name}** a quitté la salle d'attente.", ephemeral=True)
        if cog:
            asyncio.get_event_loop().call_later(3, cog._being_accepted.discard, self.waiting_member.id)
        try:
            await interaction.message.delete()
        except discord.HTTPException:
            pass

    @ui.button(label="❌ Refuser", style=discord.ButtonStyle.danger)
    async def deny(self, interaction: discord.Interaction, button: ui.Button):
        if self.handled:
            return await interaction.response.send_message("⚠️ Déjà traité.", ephemeral=True)
        if not await self._only_owner(interaction): return
        self.handled = True; self.stop()
        if self.waiting_member in self.waiting_channel.members:
            try:
                await self.waiting_member.move_to(None)
            except discord.HTTPException:
                pass
        await interaction.response.send_message(
            f"❌ **{self.waiting_member.display_name}** a été refusé.", ephemeral=True)
        try:
            await interaction.message.delete()
        except discord.HTTPException:
            pass

    async def on_timeout(self):
        if not self.handled and self.waiting_member in self.waiting_channel.members:
            try:
                await self.waiting_member.move_to(None)
            except discord.HTTPException:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# COGS
# ══════════════════════════════════════════════════════════════════════════════

class Moderation(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="ban")
    @has_mod_role()
    @commands.bot_has_permissions(ban_members=True)
    async def ban(self, ctx, member: discord.Member, *, reason: str = "Aucune raison fournie"):
        if member == ctx.author:
            return await ctx.send("❌ Tu ne peux pas te bannir toi-même.")
        if member.top_role >= ctx.author.top_role and not await self.bot.is_owner(ctx.author):
            return await ctx.send("❌ Tu ne peux pas bannir quelqu'un avec un rôle supérieur ou égal au tien.")
        await member.ban(reason=reason)
        sid = await db_add_sanction(member.id, str(member), ctx.guild.id,
                                    ctx.author.id, str(ctx.author), "ban", reason)
        await send_sanction_dm(member, "ban", reason, ctx.guild.name, sid)
        await send_sanction_log(ctx.guild, "ban", member, ctx.author, reason, sid)
        embed = discord.Embed(title="🔨 Membre banni", color=discord.Color.red(),
                              description=f"**{member}** a été banni.")
        embed.add_field(name="Raison", value=reason, inline=False)
        embed.add_field(name="Modérateur", value=ctx.author.mention, inline=True)
        embed.add_field(name="ID Sanction", value=f"#{sid}", inline=True)
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.command(name="unban")
    @has_mod_role()
    @commands.bot_has_permissions(ban_members=True)
    async def unban(self, ctx, user_id: int, *, reason: str = "Aucune raison fournie"):
        try:
            user = await self.bot.fetch_user(user_id)
            await ctx.guild.unban(user, reason=reason)
            await send_sanction_log(ctx.guild, "unban", user, ctx.author, reason, 0)
            embed = discord.Embed(title="✅ Membre débanni", color=discord.Color.green(),
                                  description=f"**{user}** a été débanni.")
            embed.add_field(name="Raison", value=reason)
            await ctx.send(embed=embed)
        except discord.NotFound:
            await ctx.send("❌ Cet utilisateur n'est pas banni ou n'existe pas.")

    @commands.command(name="kick")
    @has_mod_role()
    @commands.bot_has_permissions(kick_members=True)
    async def kick(self, ctx, member: discord.Member, *, reason: str = "Aucune raison fournie"):
        if member == ctx.author:
            return await ctx.send("❌ Tu ne peux pas te kick toi-même.")
        if member.top_role >= ctx.author.top_role and not await self.bot.is_owner(ctx.author):
            return await ctx.send("❌ Tu ne peux pas kick quelqu'un avec un rôle supérieur ou égal au tien.")
        await member.kick(reason=reason)
        sid = await db_add_sanction(member.id, str(member), ctx.guild.id,
                                    ctx.author.id, str(ctx.author), "kick", reason)
        await send_sanction_dm(member, "kick", reason, ctx.guild.name, sid)
        await send_sanction_log(ctx.guild, "kick", member, ctx.author, reason, sid)
        embed = discord.Embed(title="👢 Membre expulsé", color=discord.Color.orange(),
                              description=f"**{member}** a été expulsé.")
        embed.add_field(name="Raison", value=reason, inline=False)
        embed.add_field(name="Modérateur", value=ctx.author.mention, inline=True)
        embed.add_field(name="ID Sanction", value=f"#{sid}", inline=True)
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.command(name="timeout", aliases=["mute", "to"])
    @has_mod_role()
    @commands.bot_has_permissions(moderate_members=True)
    async def timeout(self, ctx, member: discord.Member, duration: int, *, reason: str = "Aucune raison fournie"):
        if member == ctx.author:
            return await ctx.send("❌ Tu ne peux pas te mettre en timeout toi-même.")
        if member.top_role >= ctx.author.top_role and not await self.bot.is_owner(ctx.author):
            return await ctx.send("❌ Tu ne peux pas timeout quelqu'un avec un rôle supérieur ou égal au tien.")
        if duration < 1 or duration > 40320:
            return await ctx.send("❌ La durée doit être entre 1 et 40320 minutes (28 jours).")
        until = discord.utils.utcnow() + timedelta(minutes=duration)
        await member.timeout(until, reason=reason)
        sid = await db_add_sanction(member.id, str(member), ctx.guild.id,
                                    ctx.author.id, str(ctx.author), "timeout", reason, duration)
        await send_sanction_dm(member, "timeout", reason, ctx.guild.name, sid, duration)
        await send_sanction_log(ctx.guild, "timeout", member, ctx.author, reason, sid, duration)
        embed = discord.Embed(title="⏱️ Membre en timeout", color=discord.Color.gold(),
                              description=f"**{member}** a été mis en timeout.")
        embed.add_field(name="Durée", value=f"{duration} minute(s)", inline=True)
        embed.add_field(name="Raison", value=reason, inline=False)
        embed.add_field(name="Modérateur", value=ctx.author.mention, inline=True)
        embed.add_field(name="ID Sanction", value=f"#{sid}", inline=True)
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.command(name="untimeout", aliases=["unmute", "unto"])
    @has_mod_role()
    @commands.bot_has_permissions(moderate_members=True)
    async def untimeout(self, ctx, member: discord.Member, *, reason: str = "Levée manuelle"):
        await member.timeout(None, reason=reason)
        await send_sanction_log(ctx.guild, "untimeout", member, ctx.author, reason, 0)
        await ctx.send(f"✅ Le timeout de **{member}** a été levé.")

    @commands.command(name="warn", aliases=["avertir"])
    @has_mod_role()
    async def warn(self, ctx, member: discord.Member, *, reason: str = "Aucune raison fournie"):
        if member == ctx.author:
            return await ctx.send("❌ Tu ne peux pas t'avertir toi-même.")
        sid = await db_add_sanction(member.id, str(member), ctx.guild.id,
                                    ctx.author.id, str(ctx.author), "warn", reason)
        await send_sanction_dm(member, "warn", reason, ctx.guild.name, sid)
        await send_sanction_log(ctx.guild, "warn", member, ctx.author, reason, sid)
        embed = discord.Embed(title="⚠️ Avertissement", color=discord.Color.yellow(),
                              description=f"**{member}** a reçu un avertissement.")
        embed.add_field(name="Raison", value=reason, inline=False)
        embed.add_field(name="Modérateur", value=ctx.author.mention, inline=True)
        embed.add_field(name="ID Sanction", value=f"#{sid}", inline=True)
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.command(name="purge", aliases=["clear", "nettoyer"])
    @has_mod_role()
    @commands.bot_has_permissions(manage_messages=True)
    async def purge(self, ctx, amount: int, member: discord.Member = None):
        if amount < 1 or amount > 1000:
            return await ctx.send("❌ Le nombre de messages doit être entre **1** et **1000**.", delete_after=5)
        await ctx.message.delete()
        if member:
            count = 0
            def member_check(msg):
                nonlocal count
                if msg.author == member and not msg.pinned and count < amount:
                    count += 1; return True
                return False
            deleted = await ctx.channel.purge(limit=min(amount * 10, 2000), check=member_check,
                                              bulk=True, reason=f"Purge par {ctx.author}")
        else:
            deleted = await ctx.channel.purge(limit=amount, check=lambda m: not m.pinned,
                                              bulk=True, reason=f"Purge par {ctx.author}")
        n = len(deleted)
        msg = (f"✅ **{n}** message(s) de **{member}** supprimé(s) dans {ctx.channel.mention}."
               if member else f"✅ **{n}** message(s) supprimé(s) dans {ctx.channel.mention}.")
        await ctx.send(msg, delete_after=6)
        log_ch = ctx.guild.get_channel(LOG_MESSAGES_CHANNEL_ID)
        if log_ch:
            embed = discord.Embed(title="🗑️ Purge de messages", color=discord.Color.red(),
                                  timestamp=datetime.utcnow())
            embed.add_field(name="Salon", value=ctx.channel.mention, inline=True)
            embed.add_field(name="Modérateur", value=ctx.author.mention, inline=True)
            embed.add_field(name="Messages supprimés", value=str(n), inline=True)
            if member:
                embed.add_field(name="Filtré sur", value=f"{member.mention} (`{member}`)", inline=True)
            await log_ch.send(embed=embed)

    @commands.command(name="history", aliases=["historique", "sanctions"])
    @has_mod_role()
    async def history(self, ctx, member: discord.Member = None):
        sanctions = await db_get_sanctions(ctx.guild.id, member.id if member else None)
        if not sanctions:
            target = f"de **{member}**" if member else "du serveur"
            return await ctx.send(f"📭 Aucune sanction trouvée {target}.")
        title = f"📋 Sanctions de {member}" if member else "📋 Historique des sanctions"
        embed = discord.Embed(title=title, color=discord.Color.blurple())
        for s in sanctions[:10]:
            status = "✅ Active" if s["active"] else "❌ Levée"
            dur = f" | {s['duration']}min" if s["duration"] else ""
            label = SANCTION_LABELS.get(s["type"], s["type"])
            embed.add_field(
                name=f"#{s['id']} — {label}",
                value=(f"**Membre:** {s['user_name']}\n**Raison:** {s['reason'] or 'Aucune'}\n"
                       f"**Modérateur:** {s['moderator_name']}\n"
                       f"**Date:** {s['created_at'][:16]}{dur}\n**Statut:** {status}"),
                inline=False)
        if len(sanctions) > 10:
            embed.set_footer(text=f"Affichage des 10 dernières sur {len(sanctions)} sanctions.")
        await ctx.send(embed=embed)

    @commands.command(name="delsanction", aliases=["removesanction", "supprimersanction"])
    @has_mod_role()
    async def delsanction(self, ctx, sanction_id: int):
        sanction = await db_get_sanction_by_id(sanction_id, ctx.guild.id)
        if not sanction:
            return await ctx.send(f"❌ Aucune sanction #{sanction_id} trouvée sur ce serveur.")
        if not sanction["active"]:
            return await ctx.send(f"⚠️ La sanction #{sanction_id} est déjà inactive.")
        label = SANCTION_LABELS.get(sanction["type"], sanction["type"])
        lifted_msg, reversal_ok = "", True
        if sanction["type"] == "ban":
            try:
                user = await self.bot.fetch_user(sanction["user_id"])
                await ctx.guild.unban(user, reason=f"Sanction #{sanction_id} supprimée par {ctx.author}")
                lifted_msg = "\n✅ **Le bannissement a été levé automatiquement.**"
            except discord.NotFound:
                lifted_msg = "\n⚠️ L'utilisateur n'était plus banni (déjà levé)."
            except discord.HTTPException as e:
                lifted_msg = f"\n⚠️ Impossible de lever le ban : {e}"; reversal_ok = False
        elif sanction["type"] == "timeout":
            try:
                member = ctx.guild.get_member(sanction["user_id"])
                if member is None:
                    try: member = await ctx.guild.fetch_member(sanction["user_id"])
                    except discord.NotFound: member = None
                if member is not None and member.is_timed_out():
                    await member.timeout(None, reason=f"Sanction #{sanction_id} supprimée par {ctx.author}")
                    lifted_msg = "\n✅ **Le timeout a été levé automatiquement.**"
                elif member is None:
                    lifted_msg = "\n⚠️ Membre introuvable sur le serveur."
                else:
                    lifted_msg = "\n⚠️ Le timeout était déjà expiré."
            except discord.HTTPException as e:
                lifted_msg = f"\n⚠️ Impossible de lever le timeout : {e}"; reversal_ok = False
        if reversal_ok:
            await db_deactivate_sanction(sanction_id, ctx.guild.id)
            await send_sanction_log(ctx.guild, "delsanction", sanction["user_name"], ctx.author,
                                    sanction["reason"] or "Aucune", sanction_id)
        else:
            lifted_msg += "\n❌ **La sanction reste active en base de données** car la levée Discord a échoué."
        embed = discord.Embed(
            title=f"🗑️ Sanction #{sanction_id} supprimée", color=discord.Color.green(),
            description=(f"**Type :** {label}\n**Membre :** {sanction['user_name']}\n"
                         f"**Raison originale :** {sanction['reason'] or 'Aucune'}\n"
                         f"**Supprimée par :** {ctx.author.mention}" + lifted_msg))
        await ctx.send(embed=embed)

    @ban.error
    @kick.error
    @timeout.error
    @warn.error
    @purge.error
    @history.error
    @delsanction.error
    async def mod_error(self, ctx, error):
        if isinstance(error, commands.MissingRole):
            await ctx.send("❌ Tu n'as pas la permission d'utiliser cette commande.")
        elif isinstance(error, commands.MemberNotFound):
            await ctx.send("❌ Membre introuvable. Mentionne un membre valide.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"❌ Argument invalide : {error}")
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send(f"❌ Je n'ai pas les permissions nécessaires : {error.missing_permissions}")
        else:
            await ctx.send(f"❌ Une erreur est survenue : {error}")


class TwitchNotifier(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_live = False
        self.access_token = None
        self.check_live.start()

    def cog_unload(self):
        self.check_live.cancel()

    async def get_access_token(self):
        if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
            return None
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://id.twitch.tv/oauth2/token", params={
                    "client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET,
                    "grant_type": "client_credentials"}) as r:
                    if r.status == 200:
                        return (await r.json()).get("access_token")
        except Exception as e:
            print(f"[Twitch] Erreur token : {e}")
        return None

    async def fetch_stream_data(self):
        if not self.access_token:
            self.access_token = await self.get_access_token()
        if not self.access_token:
            return None

        async def _req(token):
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
            async with aiohttp.ClientSession() as s:
                async with s.get("https://api.twitch.tv/helix/streams",
                                 headers=headers, params={"user_login": TWITCH_USERNAME}) as r:
                    return r.status, await r.json()
        try:
            status, data = await _req(self.access_token)
            if status == 401:
                self.access_token = await self.get_access_token()
                if not self.access_token: return None
                status, data = await _req(self.access_token)
            if status == 200:
                streams = data.get("data", [])
                return streams[0] if streams else None
        except Exception as e:
            print(f"[Twitch] Erreur stream : {e}")
        return None

    async def fetch_user_data(self):
        if not self.access_token: return None
        try:
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {self.access_token}"}
            async with aiohttp.ClientSession() as s:
                async with s.get("https://api.twitch.tv/helix/users",
                                 headers=headers, params={"login": TWITCH_USERNAME}) as r:
                    if r.status == 200:
                        users = (await r.json()).get("data", [])
                        return users[0] if users else None
        except Exception:
            return None

    @tasks.loop(seconds=TWITCH_CHECK_INTERVAL)
    async def check_live(self):
        await self.bot.wait_until_ready()
        try:
            stream = await self.fetch_stream_data()
            if stream is None and not self.is_live: return
            currently_live = stream is not None
            if currently_live and not self.is_live:
                self.is_live = True
                await self._send_notification(stream)
            elif not currently_live and self.is_live:
                self.is_live = False
                print(f"[Twitch] {TWITCH_USERNAME} n'est plus en live.")
        except Exception as e:
            print(f"[Twitch] Erreur boucle : {e}")

    @check_live.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()

    async def _send_notification(self, stream: dict):
        channel = self.bot.get_channel(LIVE_CHANNEL_ID)
        if not channel:
            return
        user_data = await self.fetch_user_data()
        avatar_url = user_data.get("profile_image_url") if user_data else None
        game = stream.get("game_name", "Jeu inconnu")
        title = stream.get("title", "Sans titre")
        viewer_count = stream.get("viewer_count", 0)
        stream_url = f"https://www.twitch.tv/{TWITCH_USERNAME}"
        thumbnail = stream.get("thumbnail_url", "").replace("{width}", "1280").replace("{height}", "720")
        embed = discord.Embed(
            title=f"🔴 {TWITCH_USERNAME} est en LIVE !",
            url=stream_url, description=f"**{title}**", color=0x9146FF)
        embed.add_field(name="🎮 Jeu", value=game, inline=True)
        embed.add_field(name="👥 Spectateurs", value=str(viewer_count), inline=True)
        embed.add_field(name="🔗 Lien", value=f"[Regarder le live]({stream_url})", inline=False)
        if thumbnail:
            embed.set_image(url=thumbnail + f"?t={stream.get('started_at', '')}")
        if avatar_url:
            embed.set_thumbnail(url=avatar_url)
        embed.set_footer(text="Twitch • Notification automatique")
        await channel.send(
            content=f"@everyone 🔴 **{TWITCH_USERNAME}** vient de lancer un live !",
            embed=embed)
        print(f"[Twitch] Notification envoyée pour {TWITCH_USERNAME}.")


class Planning(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="planning", aliases=["schedule", "calendrier"])
    @commands.is_owner()
    async def planning(self, ctx, *, contenu: str = None):
        if not contenu:
            return await ctx.send(
                "❌ Tu dois fournir le contenu du planning.\n**Exemple :**\n"
                "```\n<<planning Lundi 20h - Minecraft\nMercredi 21h - Fortnite\n```")
        lignes = contenu.strip().split("\n")
        titre, entries = f"📅 Planning de Stream", lignes
        if lignes[0].startswith("#"):
            titre = f"📅 {lignes[0].lstrip('#').strip()}"
            entries = lignes[1:]
        canal = self.bot.get_channel(PLANNING_CHANNEL_ID)
        if not canal:
            return await ctx.send(f"❌ Salon de planning introuvable (ID: {PLANNING_CHANNEL_ID}).")
        embed = discord.Embed(title=titre, color=0x9146FF, timestamp=datetime.utcnow())
        planning_text = ""
        for ligne in entries:
            ligne = ligne.strip()
            if not ligne: continue
            if " - " in ligne:
                p = ligne.split(" - ", 1)
                planning_text += f"🕐 **{p[0].strip()}** — {p[1].strip()}\n"
            else:
                planning_text += f"• {ligne}\n"
        if not planning_text:
            return await ctx.send("❌ Le planning est vide après traitement.")
        embed.description = planning_text
        embed.set_footer(text=f"Planning posté par {ctx.author.display_name}",
                         icon_url=ctx.author.display_avatar.url)
        try: await ctx.message.delete()
        except discord.Forbidden: pass
        await canal.send(embed=embed)
        await ctx.author.send(f"✅ Ton planning a bien été posté dans <#{PLANNING_CHANNEL_ID}> !")

    @planning.error
    async def planning_error(self, ctx, error):
        if isinstance(error, commands.NotOwner):
            await ctx.send("❌ Cette commande est réservée au propriétaire du bot.")
        else:
            await ctx.send(f"❌ Erreur : {error}")

    @commands.command(name="clearplanning", aliases=["deleteplanning"])
    @commands.is_owner()
    async def clearplanning(self, ctx, nombre: int = 10):
        canal = self.bot.get_channel(PLANNING_CHANNEL_ID)
        if not canal: return await ctx.send("❌ Salon de planning introuvable.")
        deleted = await canal.purge(limit=nombre, check=lambda m: m.author == self.bot.user)
        await ctx.send(f"✅ {len(deleted)} message(s) supprimé(s) du planning.", delete_after=5)
        try: await ctx.message.delete()
        except discord.Forbidden: pass

    @clearplanning.error
    async def clearplanning_error(self, ctx, error):
        if isinstance(error, commands.NotOwner):
            await ctx.send("❌ Cette commande est réservée au propriétaire du bot.")


class Tickets(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="panelticket", aliases=["setupticket", "ticket"])
    @commands.is_owner()
    async def panel_ticket(self, ctx):
        channel = self.bot.get_channel(TICKET_PANEL_CHANNEL_ID)
        if not channel:
            return await ctx.send(f"❌ Salon panel introuvable (ID: {TICKET_PANEL_CHANNEL_ID})")
        embed = discord.Embed(
            title="🎫 Support — Créer un ticket",
            description=(
                "Tu as besoin d'aide ou tu as un problème ?\n\n"
                "💬 **Discord** — Questions ou problèmes liés au serveur Discord\n"
                "🟣 **Twitch** — Questions ou problèmes liés au Twitch\n\n"
                "Clique sur le bouton ci-dessous pour ouvrir un ticket.\n"
                "*Un seul ticket à la fois par membre.*"),
            color=0x9146FF)
        embed.set_footer(text="Système de tickets — exotichazle")
        await channel.send(embed=embed, view=TicketPanelView())
        try: await ctx.message.delete()
        except discord.Forbidden: pass
        await ctx.send("✅ Panel de tickets posté !", delete_after=5)

    @panel_ticket.error
    async def panel_error(self, ctx, error):
        if isinstance(error, commands.NotOwner):
            await ctx.send("❌ Réservé au propriétaire du bot.")


class Logs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild: return
        if message.channel.id != IMAGES_ONLY_CHANNEL_ID: return
        has_image = any(a.content_type and a.content_type.startswith("image/") for a in message.attachments)
        if not has_image:
            try: await message.delete()
            except discord.HTTPException: pass

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if not before.guild or before.author.bot or before.content == after.content: return
        ch = before.guild.get_channel(LOG_MESSAGES_CHANNEL_ID)
        if not ch: return
        embed = discord.Embed(title="✏️ Message modifié", color=discord.Color.orange(),
                              timestamp=datetime.utcnow())
        embed.set_author(name=str(before.author), icon_url=before.author.display_avatar.url)
        embed.add_field(name="Salon", value=before.channel.mention, inline=True)
        embed.add_field(name="Auteur", value=before.author.mention, inline=True)
        embed.add_field(name="Avant", value=before.content[:1024] or "*(vide)*", inline=False)
        embed.add_field(name="Après", value=after.content[:1024] or "*(vide)*", inline=False)
        embed.add_field(name="Lien", value=f"[Aller au message]({after.jump_url})", inline=False)
        await ch.send(embed=embed)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not message.guild or message.author.bot: return
        ch = message.guild.get_channel(LOG_MESSAGES_CHANNEL_ID)
        if not ch: return
        embed = discord.Embed(title="🗑️ Message supprimé", color=discord.Color.red(),
                              timestamp=datetime.utcnow())
        embed.set_author(name=str(message.author), icon_url=message.author.display_avatar.url)
        embed.add_field(name="Salon", value=message.channel.mention, inline=True)
        embed.add_field(name="Auteur", value=message.author.mention, inline=True)
        embed.add_field(name="Contenu", value=message.content[:1024] or "*(vide)*", inline=False)
        if message.attachments:
            embed.add_field(name="Pièces jointes",
                            value="\n".join(a.filename for a in message.attachments), inline=False)
        await ch.send(embed=embed)

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user):
        guild = reaction.message.guild
        if not guild or user.bot: return
        ch = guild.get_channel(LOG_MESSAGES_CHANNEL_ID)
        if not ch: return
        embed = discord.Embed(title="😀 Réaction ajoutée", color=discord.Color.green(),
                              timestamp=datetime.utcnow())
        embed.add_field(name="Réaction", value=str(reaction.emoji), inline=True)
        embed.add_field(name="Salon", value=reaction.message.channel.mention, inline=True)
        embed.add_field(name="Par", value=user.mention, inline=True)
        embed.add_field(name="Message", value=f"[Voir]({reaction.message.jump_url})", inline=False)
        await ch.send(embed=embed)

        @commands.Cog.listener()
        async def on_member_join(self, member: discord.Member):
            print(f"[JOIN] {member} vient de rejoindre")

            ch = member.guild.get_channel(LOG_MEMBRES_CHANNEL_ID)
            if ch is None:
                print(f"[JOIN] Salon introuvable : {LOG_MEMBRES_CHANNEL_ID}")
                return

            try:
                embed = discord.Embed(
                    title="✅ Membre rejoint",
                    description=f"{member.mention} vient de rejoindre le serveur.",
                    color=discord.Color.green()
                )

                embed.add_field(
                    name="ID",
                    value=str(member.id),
                    inline=True
                )

                embed.set_thumbnail(url=member.display_avatar.url)

                await ch.send(embed=embed)
                print("[JOIN] Log envoyé avec succès")

            except Exception as error:
                print(f"[JOIN] ERREUR : {type(error).__name__}: {error}")

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        ch = member.guild.get_channel(LOG_MEMBRES_CHANNEL_ID)
        if not ch: return
        roles = [r.mention for r in member.roles if r.name != "@everyone"]
        embed = discord.Embed(title="❌ Membre parti", color=discord.Color.red(),
                              timestamp=datetime.utcnow())
        embed.set_author(name=str(member), icon_url=member.display_avatar.url)
        embed.add_field(name="Membre", value=str(member), inline=True)
        embed.add_field(name="ID", value=str(member.id), inline=True)
        embed.add_field(name=f"Rôles ({len(roles)})",
                        value=" ".join(roles)[:1024] if roles else "*(aucun)*", inline=False)
        await ch.send(embed=embed)


class VoicePrivate(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._waiting_channels: dict[int, int] = {}
        self._being_accepted: set[int] = set()

    @commands.Cog.listener()
    async def on_ready(self):
        """Supprime les canaux 🔒 orphelins (vides + sans entrée DB) au démarrage."""
        for guild in self.bot.guilds:
            for channel in list(guild.voice_channels):
                if not channel.name.startswith("🔒 "): continue
                if len(channel.members) > 0: continue
                info = await db_get_voice(channel.id)
                if info:
                    wait_name = f"⏳ {channel.name}"
                    wc = discord.utils.get(guild.voice_channels, name=wait_name, category=channel.category)
                    if wc:
                        self._waiting_channels[channel.id] = wc.id
                    continue
                try:
                    await channel.delete(reason="Nettoyage démarrage : canal privé orphelin vide")
                except discord.HTTPException:
                    pass

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member,
                                    before: discord.VoiceState, after: discord.VoiceState):
        if after.channel and after.channel.id == VOICE_CREATE_CHANNEL_ID:
            await self._create_channel(member, after.channel)
            return
        if after.channel and after.channel.id != VOICE_CREATE_CHANNEL_ID:
            info = await db_get_voice(after.channel.id)
            if info:
                from_id = before.channel.id if before.channel else None
                await self._handle_join(member, after.channel, info, from_id)
        if before.channel and before.channel.id != VOICE_CREATE_CHANNEL_ID:
            info = await db_get_voice(before.channel.id)
            if info:
                await self._handle_leave(before.channel)

    async def _create_channel(self, member: discord.Member, lobby: discord.VoiceChannel):
        guild = member.guild
        safe = "".join(c for c in member.display_name if c.isalnum() or c in " -_").strip() or "user"
        try:
            channel = await guild.create_voice_channel(name=f"🔒 {safe}", category=lobby.category)
        except discord.HTTPException:
            return
        await db_create_voice(channel.id, member.id, guild.id)
        try:
            await member.move_to(channel)
        except discord.HTTPException:
            await db_delete_voice(channel.id)
            await channel.delete(reason="Déplacement impossible")
            return
        embed = await build_voice_embed(channel.id, guild)
        msg = await channel.send(embed=embed, view=VoiceControlPanel())
        await db_set_voice_panel(channel.id, msg.id)

    async def _handle_join(self, member, channel, info, from_channel_id):
        if member.id == info["owner_id"]: return
        wait_ch_id = self._waiting_channels.get(channel.id)
        if from_channel_id and wait_ch_id and from_channel_id == wait_ch_id: return
        if member.id in self._being_accepted: return
        if await db_in_bl(channel.id, member.id):
            try:
                await member.move_to(None)
                await member.send(f"🚫 Tu es dans la blacklist du salon **{channel.name}**.")
            except discord.HTTPException:
                pass
            return
        if await db_in_wl(channel.id, member.id): return
        mode = info["mode"]
        if mode == "public":
            return
        elif mode == "private":
            try:
                await member.move_to(None)
                await member.send(f"🔒 Le salon **{channel.name}** est privé.")
            except discord.HTTPException:
                pass
        elif mode == "waiting":
            await self._send_to_waiting(member, channel, info)

    async def _send_to_waiting(self, member, channel, info):
        wait_ch = await get_or_create_waiting_channel(member.guild, channel, self)
        if not wait_ch: return
        try:
            await member.move_to(wait_ch)
        except discord.HTTPException:
            return
        owner = member.guild.get_member(info["owner_id"])
        embed = discord.Embed(
            title="⏳ Demande d'accès",
            description=f"{member.mention} veut rejoindre ton salon vocal.",
            color=discord.Color.orange(), timestamp=datetime.utcnow())
        embed.set_thumbnail(url=member.display_avatar.url)
        await channel.send(
            content=owner.mention if owner else "",
            embed=embed, view=WaitingApprovalView(channel.id, member, wait_ch))

    async def _handle_leave(self, channel: discord.VoiceChannel):
        channel_id = channel.id
        await asyncio.sleep(1)
        channel = channel.guild.get_channel(channel_id)
        if channel is None or len(channel.members) == 0:
            if channel:
                await delete_waiting_channel(channel.guild, channel_id, self)
                try:
                    await channel.delete(reason="Salon privé vide")
                except discord.HTTPException:
                    pass
            self._waiting_channels.pop(channel_id, None)
            await db_delete_voice(channel_id)


# ══════════════════════════════════════════════════════════════════════════════
# BOT & POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

class DiscordBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        intents.reactions = True
        super().__init__(command_prefix=PREFIX, intents=intents,
                         case_insensitive=True, help_command=None)

    async def setup_hook(self):
        await init_db()
        # Vues persistantes enregistrées avant les cogs
        self.add_view(TicketPanelView())
        self.add_view(TicketCloseView())
        self.add_view(VoiceControlPanel())
        # Chargement direct des cogs (pas besoin de load_extension)
        for CogClass in (Moderation, TwitchNotifier, Planning, Tickets, Logs, VoicePrivate):
            await self.add_cog(CogClass(self))
            print(f"[Bot] ✅ Cog chargé : {CogClass.__name__}")

    class DiscordBot(commands.Bot):
        def __init__(self):
            # code existant...
            pass

        async def setup_hook(self):
            # code existant...
            pass

        async def on_socket_event_type(self, event_type):
            if event_type in ("GUILD_MEMBER_ADD", "GUILD_MEMBER_REMOVE"):
                print(f"[DISCORD] Événement reçu : {event_type}")

        async def on_ready(self):
            print(f"[Bot] Connecté en tant que {self.user} (ID: {self.user.id})")
    
    async def on_ready(self):
        print(f"[Bot] Connecté en tant que {self.user} (ID: {self.user.id})")
        await self.change_presence(
            activity=discord.Activity(type=discord.ActivityType.watching, name="le serveur 👀"))

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandNotFound): return
        if isinstance(error, commands.NoPrivateMessage):
            await ctx.send("❌ Cette commande ne peut pas être utilisée en message privé.")
        elif isinstance(error, commands.CheckFailure):
            await ctx.send("❌ Tu n'as pas la permission d'utiliser cette commande.")


async def main():
    if not DISCORD_TOKEN:
        print("[Bot] ❌ DISCORD_TOKEN manquant. Configure tes secrets.")
        sys.exit(1)

    bot = DiscordBot()

    @bot.command(name="help", aliases=["aide"])
    async def help_cmd(ctx):
        embed = discord.Embed(title="📖 Commandes du bot", color=0x9146FF)
        embed.add_field(name="🔨 Modération", inline=False, value=(
            f"`{PREFIX}ban @membre [raison]` — Bannir\n"
            f"`{PREFIX}unban <id>` — Débannir\n"
            f"`{PREFIX}kick @membre [raison]` — Expulser\n"
            f"`{PREFIX}timeout @membre <min> [raison]` — Timeout\n"
            f"`{PREFIX}untimeout @membre` — Lever un timeout\n"
            f"`{PREFIX}warn @membre [raison]` — Avertir\n"))
        embed.add_field(name="📋 Historique (modérateurs)", inline=False, value=(
            f"`{PREFIX}history [@membre]` — Voir les sanctions\n"
            f"`{PREFIX}delsanction <id>` — Supprimer + lever ban/timeout\n"))
        embed.add_field(name="🎫 Tickets", inline=False, value=(
            f"`{PREFIX}panelticket` — Poster le panel de tickets *(owner)*\n"
            "Les tickets se créent via le bouton dans le salon dédié.\n"))
        embed.add_field(name="📅 Planning (propriétaire)", inline=False, value=(
            f"`{PREFIX}planning <contenu>` — Envoyer un planning\n"
            f"`{PREFIX}clearplanning [n]` — Nettoyer le salon planning\n"))
        embed.set_footer(text=f"Préfixe : {PREFIX} | Bot — exotichazle")
        await ctx.send(embed=embed)

    # ── Serveur HTTP keep-alive (requis pour Render Web Service) ──────────────
    from aiohttp import web as aiohttp_web

    async def health(request):
        return aiohttp_web.Response(text="✅ Bot en ligne", content_type="text/plain")

    app = aiohttp_web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    port = int(os.environ.get("PORT", 5000))
    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[Web] ✅ Serveur HTTP démarré sur le port {port}")

    async with bot:
        await bot.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
