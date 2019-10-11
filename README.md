
# Message Channel

## Get Start

* Add it in your root build.gradle at the end of repositories:

    ```gradle
    allprojects {
        repositories {
          ...
          maven { url 'https://jitpack.io' }
        }
    }
    ```

* Add the dependency:

    ```gradle
    dependencies {
    }
    ```

## Usage

1. Subscribe simple:

    ```kotlin
    val channel: Channel = WebSocketChannel("wss://echo.websocket.org")
    channel.onMessage = { println(it) }
    channel.send("hello world!")
    Thread.sleep(2000)
    channel.close()
    ```

2. Subscribe with channel id:

    ```kotlin
    val channel: Channel = WebSocketChannel("wss://echo.websocket.org")
    channel.onMessage = { println(it) }

    val input = SubscribeInput(
      mapSubscribe = { operation, channelId ->
        when (operation) {
          SubscribeInput.Operation.SUBSCRIBE -> "{\"op\":\"subscribe\",\"channel\":\"$channelId\"}"
          SubscribeInput.Operation.UNSUBSCRIBE -> "{\"op\":\"unsubscribe\",\"channel\":\"$channelId\"}"
        }
      }
    )
    input.add("hello_message")

    channel.addChannelListener(input)
    channel.prepare()
    Thread.sleep(2000)
    channel.close()
    ```

## License

```

   Copyright(c) 2019 VerstSiu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

```