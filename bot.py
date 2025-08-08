import discord
from discord.ext import commands
import json
import re
import asyncio
import os

Intents = discord.Intents.all()

channels = []
roles = []

def read (filename):
   try:
        with open(filename, 'r') as json_file:
            return json.loads(json_file.read())
   except FileNotFoundError:
        return []

def write(filename , save_object):
    with open(filename, 'w') as json_file:
        json_file.write(json.dumps(save_object))

channels = read('/config/channels.json')#.get('channels', None)
if channels is None:
    channels = []
	
roles = read('/config/roles.json')#.get('roles', None)
if roles is None:
    roles = []
	
bot = commands.Bot(command_prefix='!', description="This is a Fic notification Bot",intents=Intents)

@bot.command()
async def add(ctx, *, msg):
    if str(ctx.channel.id) not in channels:
        channels.append(str(ctx.channel.id))
        roles.append(msg)
        write('/config/channels.json', channels)
        write('/config/roles.json',  roles)

# Events
@bot.event
async def on_message(message):
    if (message.attachments and (str(message.channel.id) in channels)):
        s = message.attachments[0].url
        if s.split("?")[0].lower().endswith(('.epub', '.txt', '.docx', '.pdf')):
            ind = channels.index(str(message.channel.id))
            role = discord.utils.get(message.guild.roles, name=roles[ind])
            await message.channel.send(role.mention)
            pins = await message.channel.pins()
            if (len(pins) > 48):
               await pins[-1].unpin()
            await message.pin(reason=None)
    else:
        if (message.type == discord.MessageType.pins_add):
            await message.delete()
        await bot.process_commands(message)

@bot.event
async def on_ready():
    game = discord.Game("with channels")
    await bot.change_presence(status=discord.Status.idle, activity=game)
    print('Bot has started')

bot.run(os.getenv('discord_token'))
