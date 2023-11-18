from asyncio import sleep
from os import mkdir
from pathlib import Path

from pyaml_env import parse_config
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import MsgIdInvalidError
from telethon.tl.types import Channel, ChannelForbidden, Message

config = parse_config(".env.yaml")

API_ID = config["API_ID"]
assert API_ID != None
API_HASH = config["API_HASH"]
assert API_HASH != None
CHANNEL_NAME = config["CHANNEL_NAME"]
assert CHANNEL_NAME != None


DIRECTORY_TO_SAVE = Path("./posts")
picked_channel_id_by_name: str | None = None


def _get_reactions(post_or_comment) -> str:
    reactions: list[tuple[str, int]] = []
    if post_or_comment.reactions:
        for reaction in post_or_comment.reactions.results:
            reactions.append(
                (
                    reaction.reaction.emoticon
                    if hasattr(reaction.reaction, "emoticon")
                    else "(CustomEmoji)",
                    reaction.count,
                )
            )
    reactions.sort(key=lambda a: a[1], reverse=True)
    return "; ".join([f"{item[0]} {item[1]}" for item in reactions])


async def _get_sendled(comment) -> str:
    from_user_or_channel = await comment.get_sender()
    if isinstance(from_user_or_channel, Channel):
        return f"[{from_user_or_channel.title}](https://t.me/{from_user_or_channel.username})]"
    elif isinstance(from_user_or_channel, ChannelForbidden):
        return f"[{from_user_or_channel.title}](ChannelForbiddenID={from_user_or_channel.id})]"

    if from_user_or_channel:
        return f"[{from_user_or_channel.first_name if from_user_or_channel.first_name else ''}{' ' + from_user_or_channel.last_name if from_user_or_channel.last_name else ''}](https://t.me/{from_user_or_channel.username})"
    return "[Channel](t.me/?)"


def _text_replace_quotes(text) -> str:
    return text.replace("```", "\n```\n")


async def _get_md_post(post) -> str:
    md_post = ""
    md_post += f"## Пост от {post.date.strftime('%d.%m.%Y, %H:%M:%S')}\n"
    md_post += (
        (_text_replace_quotes(post.text) if post.text else "{no text}")
        + "\n"
        + _get_reactions(post)
        + "\n\n"
    )

    try:
        md_post += "### Комментарии" + "\n"
        async for comment in client.iter_messages(
            CHANNEL_NAME, reply_to=post.id, reverse=True
        ):
            md_post += (
                f"#### {await _get_sendled(comment)}"
                + " "
                + comment.date.strftime("%d.%m.%Y, %H:%M:%S")
                + "\n"
            )

            if comment.reply_to.quote_text:
                md_post += "> " + comment.reply_to.quote_text + "\n\n"
            md_post += (
                (_text_replace_quotes(comment.text) if comment.text else "{no text}")
                + "\n"
                + _get_reactions(comment)
                + "\n\n"
            )
    except MsgIdInvalidError:
        print("MsgIdInvalidError")

    return md_post


def _get_meta(post) -> str:
    md_meta = ""
    md_meta += (
        "---\n"
        + f"channel: {CHANNEL_NAME}\n"
        + f"url: https://t.me/c/{picked_channel_id_by_name}/{post.id}\n"
        + f"created-at: {post.date.strftime('%Y-%m-%dT%H:%M:%S')}"
        + "\n---"
    )
    return md_meta


async def _process_one_post(post) -> None:
    md_post = await _get_md_post(post)
    md_meta = _get_meta(post)

    with open(DIRECTORY_TO_SAVE / f"{post.id}.md", "a") as f:
        f.write(md_meta)
        f.write("\n\n\n")
        f.write(md_post)


async def make_parse(client, limit: int, from_post_id: int = 0, delay: float = 0.5):
    async for post in client.iter_messages(
        CHANNEL_NAME, limit=limit, reverse=True, offset_id=from_post_id
    ):
        try:
            print("Processing", post.id)
            if not isinstance(post, Message):
                print("skip")
                continue
            await _process_one_post(post)
            await sleep(delay)
        except ValueError:
            print(f"Error, trying {post.id} one more time after 10s sleep")
            await sleep(10)
            await make_parse(client, limit, post.id - 1, delay)


async def main(client: TelegramClient) -> None:
    global picked_channel_id_by_name

    me = await client.get_me()

    print("username:", me.username)
    print("phone:", me.phone, end="\n\n")

    print("the dialogs/conversations that you are part of:")
    async for dialog in client.iter_dialogs():
        print(f"- {dialog.name}: {dialog.id}")
        if CHANNEL_NAME == dialog.name:
            picked_channel_id_by_name = str(dialog.id).removeprefix("-100")
    assert (
        picked_channel_id_by_name != None
    ), f"Канал {CHANNEL_NAME} не найден в списке каналов!"

    await make_parse(client, limit=1000, from_post_id=0, delay=0.5)


if __name__ == "__main__":
    client = TelegramClient("telethon_user", API_ID, API_HASH)

    DIRECTORY_TO_SAVE.mkdir(exist_ok=True)
    with client:
        client.loop.run_until_complete(main(client))
